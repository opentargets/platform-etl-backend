import $file.common
import common._

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import better.files.Dsl._
import better.files._
import com.typesafe.config.{Config, ConfigFactory, ConfigObject, ConfigRenderOptions}
import com.typesafe.scalalogging.{LazyLogging, Logger}

import scala.math.pow

object AssociationHelpers {
  implicit class AggregationHelpers(df: DataFrame)(implicit ss: SparkSession) {
    import Configuration._
    import ss.implicits._

    def computeOntologyExpansion(diseases: DataFrame, otc: AssociationsSection): DataFrame = {
      val diseaseStruct =
        """
          |named_struct(
          | 'id', disease_id,
          | 'efo_info', named_struct(
          |   'efo_id', code,
          |   'label', label,
          |   'path', path_codes,
          |   'therapeutic_area', named_struct(
          |     'codes', therapeutic_codes,
          |     'labels', therapeutic_labels
          |   )
          | )
          |) as disease
          |""".stripMargin

      // generate needed fields as descendants
      val lut = diseases
        .selectExpr("disease_id as did", "descendants", diseaseStruct)
        .withColumn("descendant", explode(col("descendants")))
        .drop("descendants")
        .orderBy(col("descendant"))

      // map datasource list to a dataset
      val datasources = broadcast(otc.dataSources.toDS().orderBy($"id".asc))

      /*
       ontology propagation happens just when datasource is not one of the banned ones
       by configuration file application.conf when the known datasource is specified
       */
      val dfWithLut = df
        .withColumn("disease_id", expr("disease.id"))
        .drop("disease")
        .join(
          broadcast(datasources.selectExpr("id as dsID", "propagate").orderBy("dsID")),
          col("dsID") === col("sourceID"),
          "left_outer"
        )
        .na
        .fill(otc.defaultPropagate, Seq("propagate"))
        .drop("dsID")
        .persist()

      val dfProp = dfWithLut
        .where(col("propagate") === true)
        .join(broadcast(lut), col("disease_id") === col("descendant"), "inner")

      val dfNoProp = dfWithLut
        .where(col("propagate") === false)
        .join(broadcast(lut.orderBy(col("did"))), col("disease_id") === col("did"), "inner")

      dfProp
        .unionByName(dfNoProp)
        .drop("disease_id", "descendant")
        .withColumnRenamed("did", "disease_id")
    }

    def groupByDataSources(
        datasources: Dataset[DataSource],
        otc: AssociationsSection
    ): DataFrame = {
      df.withColumn("disease_id", $"disease.id")
        .withColumn("target_id", $"target.id")
        .withColumn("_score", $"scores.association_score")
        .groupBy($"disease_id", $"target_id", $"sourceID")
        .agg(
          slice(sort_array(collect_list($"_score"), false), 1, 100).as("_v"),
          first($"target").as("target"),
          first($"disease").as("disease"),
          count($"id").as("_count")
        )
        .withColumn("datasource_count", expr("map(sourceID, _count)"))
        .harmonic("_hs", "_v")
        .join(datasources, $"sourceID" === datasources("id"), "left_outer")
        // fill null for weight to default weight in case we have new datasources
        .na
        .fill(otc.defaultWeight, Seq("weight"))
        .withColumn("_score", $"_hs" * $"weight")
        .withColumn("datasource_score", expr("map(sourceID, _score)"))
    }

    def groupByDataTypes: DataFrame = {
      df.groupBy(
          $"disease_id",
          $"target_id",
          $"dataType"
        )
        .agg(
          first($"target").as("target"),
          first($"disease").as("disease"),
          collect_list($"datasource_count").as("datasource_counts"),
          collect_list($"datasource_score").as("datasource_scores")
        )
        .withColumn("_v", expr("flatten(transform(datasource_scores, x -> map_values(x)))"))
        .harmonic("_hs", "_v")
        .withColumn(
          "datatype_count",
          expr(
            "map(dataType, aggregate(flatten(transform(datasource_counts, x -> map_values(x))) ,0D, (a, el) -> a + el))"
          )
        )
        .withColumn("datatype_score", expr("map(dataType, _hs)"))
    }

    def groupByPair: DataFrame = {
      df.groupBy(
          $"disease_id",
          $"target_id"
        )
        .agg(
          first($"target").as("target"),
          first($"disease").as("disease"),
          flatten(collect_list($"datasource_counts")).as("datasource_counts"),
          flatten(collect_list($"datasource_scores")).as("datasource_scores"),
          collect_list($"datatype_count").as("datatype_counts"),
          collect_list($"datatype_score").as("datatype_scores")
        )
        .withColumn("id", concat_ws("-", $"target_id", $"disease_id"))
        .withColumn(
          "datasource_scores",
          expr(
            "map_from_arrays(flatten(transform(datasource_scores, x -> map_keys(x))), flatten(transform(datasource_scores, x -> map_values(x))))"
          )
        )
        .withColumn(
          "datatype_scores",
          expr(
            "map_from_arrays(flatten(transform(datatype_scores, x -> map_keys(x))), flatten(transform(datatype_scores, x -> map_values(x))))"
          )
        )
        .withColumn(
          "datasource_counts",
          expr(
            "map_from_arrays(flatten(transform(datasource_counts, x -> map_keys(x))), flatten(transform(datasource_counts, x -> map_values(x))))"
          )
        )
        .withColumn(
          "datatype_counts",
          expr(
            "map_from_arrays(flatten(transform(datatype_counts, x -> map_keys(x))), flatten(transform(datatype_counts, x -> map_values(x))))"
          )
        )
        .withColumn("_v", expr("map_values(datasource_scores)"))
        .harmonic("overall", "_v")
        .withColumn(
          "evidence_count",
          expr(
            "named_struct('datasources', datasource_counts, " +
              "'datatypes', datatype_counts, 'total', aggregate(map_values(datasource_counts), 0D, (agg, el) -> agg + el))"
          )
        )
        .withColumn(
          "harmonic_sum",
          expr(
            "named_struct('datasources', datasource_scores, " +
              "'datatypes', datatype_scores, 'overall', overall)"
          )
        )
        .drop(
          "target_id",
          "disease_id",
          "_v",
          "_hs_max",
          "datasource_counts",
          "datatype_counts",
          "datasource_scores",
          "datatype_scores",
          "overall"
        )
    }
  }

  implicit class HSHelpers(df: DataFrame) {
    def harmonic(newColName: String, vectorColName: String): DataFrame = {
      val maxVectorElementsDefault: Int = 100
      val pExponentDefault: Int = 2

      def maxHarmonicValue(vSize: Int, pExponent: Int, maxScore: Double): Double =
        (0 until vSize).foldLeft(0d)((acc: Double, n: Int) =>
          acc + (maxScore / pow(1d + n, pExponent))
        )

      val maxHS = maxHarmonicValue(maxVectorElementsDefault, pExponentDefault, 1.0)
      df.withColumn(
        newColName,
        expr(s"""
                |aggregate(
                | zip_with(
                |   $vectorColName,
                |   sequence(1, size($vectorColName)),
                |   (e, i) -> (e / pow(i,2))
                | ),
                | 0D,
                | (a, el) -> a + el
                |) / $maxHS
                |""".stripMargin)
      )
    }
  }
}

object Loaders extends LazyLogging {
  def loadTargets(input: Configuration.InputInfo)(implicit ss: SparkSession): DataFrame = {
    logger.info("load targets jsonl")
    val targets = ss.read.format(input.format).load(input.path)
    targets
  }

  def loadDiseases(input: Configuration.InputInfo)(implicit ss: SparkSession): DataFrame = {
    logger.info("load diseases jsonl")
    val diseaseList = ss.read.format(input.format).load(input.path)

    // generate needed fields as ancestors
    val efos = diseaseList
      .withColumn("disease_id", substring_index(col("code"), "/", -1))
      .withColumn("ancestors", flatten(col("path_codes")))

    // compute descendants
    val descendants = efos
      .where(size(col("ancestors")) > 0)
      .withColumn("ancestor", explode(col("ancestors")))
      // all diseases have an ancestor, at least itself
      .groupBy("ancestor")
      .agg(collect_set(col("disease_id")).as("descendants"))
      .withColumnRenamed("ancestor", "disease_id")

    val diseases = efos.join(descendants, Seq("disease_id"))
    diseases
  }

  def loadEvidences(input: Configuration.InputInfo)(implicit ss: SparkSession): DataFrame = {
    logger.info("load evidences jsonl")
    val evidences = ss.read.format(input.format).load(input.path)
    evidences
  }
}

object Associations extends LazyLogging {
  def apply(config: Config)(implicit ss: SparkSession) = {
    val associationsSec = Configuration.loadAssociationSection(config)
    val commonSec = Configuration.loadCommon(config)

    import ss.implicits._
    import AssociationHelpers._

    val datasources = broadcast(associationsSec.dataSources.toDS().orderBy($"id".asc))

    val targets = Loaders.loadTargets(commonSec.inputs.target)
    val diseases = Loaders.loadDiseases(commonSec.inputs.disease)
    //  val expressions = Loaders.loadExpressions(expressionFilename)
    val evidences = Loaders.loadEvidences(commonSec.inputs.evidence)

    val directPairs = evidences
      .groupByDataSources(datasources, associationsSec)
      .groupByDataTypes
      .groupByPair
      .withColumn("is_direct", lit(true))

    // compute indirect
    val indirectPairs = evidences
      .computeOntologyExpansion(diseases, associationsSec)
      .groupByDataSources(datasources, associationsSec)
      .groupByDataTypes
      .groupByPair
      .withColumn("is_direct", lit(false))

    // write to jsonl both direct and indirect
    directPairs.write.json(commonSec.output + "/direct/")
    indirectPairs.write.json(commonSec.output + "/indirect/")
  }
}
