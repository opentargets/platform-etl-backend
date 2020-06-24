package io.opentargets.etl.backend

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import better.files.Dsl._
import better.files._
import com.typesafe.config.{Config, ConfigFactory, ConfigObject, ConfigRenderOptions}
import com.typesafe.scalalogging.{LazyLogging, Logger}
import io.opentargets.etl.backend.SparkHelpers.IOResourceConfig
import org.apache.spark.sql.expressions._

import scala.math.pow

object AssociationHelpers {
  def harmonic(vectorColName: String,
               maxVectorSize: Int = 100,
               maxComponentScore: Double = 1d,
               pExponent: Int = 2): Column = {
    def maxHarmonicValue(vSize: Int, exp: Int, maxScore: Double): Double =
      (0 until vSize).foldLeft(0d)((acc: Double, n: Int) =>
        acc + (maxScore / pow(1d + n, exp)))

    val maxHS = maxHarmonicValue(maxVectorSize, pExponent, maxComponentScore)
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
            |""".stripMargin
    )
  }

  implicit class Helpers(df: DataFrame)(implicit ss: SparkSession) {
    import Configuration._
    import ss.implicits._

    def computeOntologyExpansion(diseases: DataFrame, otc: AssociationsSection): DataFrame = {
      // generate needed fields as descendants
      val diseaseCols = Seq(
        "id as did",
        "ancestors"
      )
      val lut = diseases
        .selectExpr(diseaseCols:_*)

      // map datasource list to a dataset
      val datasources = broadcast(otc.dataSources.toDS()
        .selectExpr("id as datasource_id", "propagate")
        .orderBy($"datasource_id".asc)
      )

      /*
       ontology propagation happens just when datasource is not one of the banned ones
       by configuration file application.conf when the known datasource is specified
       */
      val dfWithLut = df
        .join(datasources, Seq("datasource_id"),
          "left_outer"
        )
        .na
        .fill(otc.defaultPropagate, Seq("propagate"))
        .repartitionByRange($"disease_id".asc)
        .persist()

      val fullExpanded = dfWithLut
        .join(broadcast(lut.orderBy($"did".asc)), $"disease_id" === $"did", "inner")
        .withColumn("_ancestors",
          when($"propagate" === true, concat(array($"did"), $"ancestors"))
            .otherwise(array($"did")))
        .withColumn("ancestor", explode($"_ancestors"))
        .drop("disease_id", "did", "ancestors", "_ancestors")
        .withColumnRenamed("ancestor", "disease_id")
        .repartitionByRange($"disease_id".asc)

      fullExpanded.persist()
    }

    def groupByDataTypes(otc: AssociationsSection): DataFrame = {
      val outputCols = Seq(
        "target_id",
        "disease_id",
        "overall_hs_score_from_harmonic",
        "overall_hs_score_from_llr"
//        "datasource_evidence_count",
//        "datasource_evidence_sum",
//        "datasource_score_harmonic",
//        "datasource_score_llr",
//        "datasource_score_llr_norm"
      )

      // obtain weights per datasource table
      val datasourceWeights = broadcast(otc.dataSources.toDS()).toDF
        .withColumnRenamed("id", "datasource_id")
        .select("datasource_id", "weight")
        .orderBy($"datasource_id".asc)

      val dtAssocs = df
        .join(datasourceWeights, Seq("datasource_id"), "left_outer")
        // fill null for weight to default weight in case we have new datasources
        .na
        .fill(otc.defaultWeight, Seq("weight"))
        .withColumn("datasource_score_llr_norm_w", $"datasource_score_llr_norm" * $"weight")
        .withColumn("datasource_score_harmonic_w", $"datasource_score_harmonic" * $"weight")

//      val wdth = Window.partitionBy($"disease_id", $"target_id").orderBy($"overall_hs_score_from_harmonic".desc_nulls_last)
//      val wdtl = Window.partitionBy($"disease_id", $"target_id").orderBy($"overall_hs_score_from_llr".desc_nulls_last)
      val dtAssocsDirect = dtAssocs
        .groupBy($"disease_id", $"target_id")
        .agg(
          slice(
            sort_array(
              collect_list(col("datasource_score_harmonic_w")),
              false
            ), 1, 100
          ).as("datasource_score_harmonic_v"),
          slice(
            sort_array(
              collect_list(col("datasource_score_llr_norm_w")),
              false
            ), 1, 100
          ).as("datasource_score_llr_norm_v")
        )
        .withColumn("overall_hs_score_from_harmonic",
          harmonic("datasource_score_harmonic_v"))
        .withColumn("overall_hs_score_from_llr",
          harmonic("datasource_score_llr_norm_v"))

      dtAssocsDirect.select(outputCols.head, outputCols.tail:_*)
    }

    def groupByDataSources(
        datasources: Dataset[DataSource],
        otc: AssociationsSection
    ): DataFrame = {
      val outputCols = Seq(
        "datatype_id",
        "datasource_id",
        "target_id",
        "disease_id",
        "A", "B", "C", "D",
        "aterm", "cterm", "acterm",
        "datasource_evidence_count",
        "datasource_evidence_sum",
        "datasource_score_harmonic",
        "datasource_score_llr",
        "datasource_score_llr_norm"
      )

      val wds = Window.partitionBy(col("datatype_id"), col("datasource_id"))
      val wt = Window.partitionBy(col("datatype_id"), col("datasource_id"), col("target_id"))
      val wd = Window.partitionBy(col("datatype_id"), col("datasource_id"), col("disease_id"))
      val wtd = Window.partitionBy(col("datatype_id"), col("datasource_id"), col("target_id"), col("disease_id"))

      val datasourceAssocs = df.withColumn("ds_count", count("evidence_id").over(wds))
        .withColumn("ds_sum", sum("evidence_score").over(wds))
        .withColumn("ds_t_sum", sum(col("evidence_score")).over(wt))
        .withColumn("ds_d_sum", sum(col("evidence_score")).over(wd))
        .withColumn("ds_td_sum", sum(col("evidence_score")).over(wtd))
        .withColumn("ds_td_count", count(col("evidence_id")).over(wtd))
        .groupBy($"datatype_id", $"datasource_id", $"disease_id", $"target_id")
        .agg(
          first(col("ds_td_sum")).as("A"),
          first(col("ds_t_sum")).as("uniq_reports_t"),
          first(col("ds_d_sum")).as("uniq_reports_d"),
          first(col("ds_sum")).as("datasource_evidence_sum"),
          first(col("ds_count")).as("datasource_evidence_count"),
          first(col("ds_td_count")).as("A_count"),
          slice(
            sort_array(
              collect_list(col("evidence_score")),
              false
            ), 1, 100
          ).as("datasource_td_score_v")
        )
        .withColumn("datasource_score_harmonic", harmonic("datasource_td_score_v"))
        .withColumn("C", col("uniq_reports_t") - col("A"))
        .withColumn("B", col("uniq_reports_d") - col("A"))
        .withColumn(
          "D",
          col("datasource_evidence_sum") - col("uniq_reports_t") - col("uniq_reports_d") + col("A")
        )
        .withColumn("aterm", $"A" * (log($"A") - log($"A" + $"B")))
        .withColumn("cterm", $"C" * (log($"C") - log($"C" + $"D")))
        .withColumn("acterm", ($"A" + $"C") * (log($"A" + $"C") - log($"A" + $"B" + $"C" + $"D")))
        .withColumn("llr", $"aterm" + $"cterm" - $"acterm")
        .withColumn("datasource_score_llr",
          when(col("llr").isNotNull and !col("llr").isNaN, $"llr")
            .otherwise(lit(0d)))
        .withColumn("datasource_score_llr_max", max(col("datasource_score_llr")).over(wds))
        .withColumn("datasource_score_llr_norm", $"datasource_score_llr" / $"datasource_score_llr_max")

      datasourceAssocs
        .select(outputCols.head, outputCols.tail:_*)
    }
  }
}

object Association extends LazyLogging {

  def computeDirectAssociations()(implicit context: ETLSessionContext): Map[String, DataFrame] = {
    implicit val ss = context.sparkSession
    import ss.implicits._
    import AssociationHelpers._

    val commonSec = context.configuration.common

    val mappedInputs = Map(
      "evidences" -> IOResourceConfig(
        commonSec.inputs.evidence.format,
        commonSec.inputs.evidence.path
      )
    )
    val dfs = SparkHelpers.readFrom(mappedInputs)

    val evidenceColumns = Seq(
      "disease.id as disease_id",
      "target.id as target_id",
      "scores.association_score as evidence_score",
      "`type` as datatype_id",
      "sourceID as datasource_id",
      "id as evidence_id"
    )

    val evidenceSet = dfs("evidences")
      .selectExpr(evidenceColumns:_*)
      .where($"evidence_score" > 0D)

    val associationsPerDS = computeAssociationsPerDS(evidenceSet)
    val associationsOverall = computeAssociationsAllDS(associationsPerDS)

    Map(
      "associations_per_datasource_direct" -> associationsPerDS,
      "associations_overall_direct" -> associationsOverall
    )

  }

  def computeIndirectAssociations()(implicit context: ETLSessionContext): Map[String, DataFrame] = {
    implicit val ss = context.sparkSession
    import ss.implicits._
    import AssociationHelpers._

    val commonSec = context.configuration.common
    val associationsSec = context.configuration.associations

    val mappedInputs = Map(
      "evidences" -> IOResourceConfig(
        commonSec.inputs.evidence.format,
        commonSec.inputs.evidence.path
      )
    )
    val dfs = SparkHelpers.readFrom(mappedInputs)

    val evidenceColumns = Seq(
      "disease.id as disease_id",
      "target.id as target_id",
      "scores.association_score as evidence_score",
      "`type` as datatype_id",
      "sourceID as datasource_id",
      "id as evidence_id"
    )

    val diseases = Disease.compute()

    val evidenceSet = dfs("evidences")
      .selectExpr(evidenceColumns:_*)
      .where($"evidence_score" > 0D)
      .computeOntologyExpansion(diseases, associationsSec)

    // associations_per_datasource_direct
    // associations_overall_direct
    val associationsPerDS = computeAssociationsPerDS(evidenceSet)
    val associationsOverall = computeAssociationsAllDS(associationsPerDS)

    Map(
      "associations_per_datasource_indirect" -> associationsPerDS,
      "associations_overall_indirect" -> associationsOverall
    )
  }

  def computeAssociationsAllDS(assocsPerDS: DataFrame)(implicit context: ETLSessionContext): DataFrame = {
    implicit val ss = context.sparkSession

    val associationsSec = context.configuration.associations
    val commonSec = context.configuration.common

    import ss.implicits._
    import AssociationHelpers._
    // compute diseases from the ETL disease step
    val diseases = Disease.compute()

    assocsPerDS.groupByDataTypes(associationsSec)
  }

  def computeAssociationsPerDS(evidences: DataFrame)(implicit context: ETLSessionContext): DataFrame = {
    implicit val ss = context.sparkSession

    val associationsSec = context.configuration.associations
    val commonSec = context.configuration.common

    import ss.implicits._
    import AssociationHelpers._

    val datasources = broadcast(associationsSec.dataSources.toDS().orderBy($"id".asc))

    evidences
      .groupByDataSources(datasources, associationsSec)
      .repartitionByRange($"disease_id")
  }

  def apply()(implicit context: ETLSessionContext) = {
    implicit val ss = context.sparkSession
    val commonSec = context.configuration.common

    val directs = computeDirectAssociations()
    val indirects = computeIndirectAssociations()

    val outputDFs = directs ++ indirects

    val outputs = outputDFs.keys map (name =>
      name -> IOResourceConfig(commonSec.outputFormat, commonSec.output + s"/$name"))

    SparkHelpers.writeTo(outputs.toMap, outputDFs)
  }
}
