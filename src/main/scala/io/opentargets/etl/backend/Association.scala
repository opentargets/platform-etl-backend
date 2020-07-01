package io.opentargets.etl.backend

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{pow => powCol}
import better.files.Dsl._
import better.files._
import com.typesafe.config.{Config, ConfigFactory, ConfigObject, ConfigRenderOptions}
import com.typesafe.scalalogging.{LazyLogging, Logger}
import io.opentargets.etl.backend.SparkHelpers.IOResourceConfig
import org.apache.spark.sql.expressions._

import scala.math.pow

object AssociationHelpers extends LazyLogging {
  def maxHarmonicValue(vSize: Int, exp: Int, maxScore: Double): Double =
    (0 until vSize).foldLeft(0d)((acc: Double, n: Int) =>
      acc + (maxScore / pow(1d + n, exp)))

  def maxHarmonicValueExpr(vsizeCol: String): Column =
    expr(s"""
            |aggregate(
            | zip_with(
            |   array_repeat(1.0, $vsizeCol),
            |   sequence(1, size($vsizeCol)),
            |   (e, i) -> (e / pow(i,2))
            | ),
            | 0D,
            | (a, el) -> a + el
            |)
            |""".stripMargin)

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

      val fullExpanded = dfWithLut
        .join(broadcast(lut.orderBy($"did".asc)), $"disease_id" === $"did", "inner")
        .withColumn("_ancestors",
          when($"propagate" === true, concat(array($"did"), $"ancestors"))
            .otherwise(array($"did")))
        .withColumn("ancestor", explode($"_ancestors"))
        .drop("disease_id", "did", "ancestors", "_ancestors")
        .withColumnRenamed("ancestor", "disease_id")

      fullExpanded
    }

    def llrOver(setA: Set[String], setB: Set[String], scoreColNames: Seq[String],
                idName: String,
                otc: Option[AssociationsSection]): DataFrame = {
      require((setA intersect setB) nonEmpty, logger.error("intersection column sets must be non empty"))

      // obtain weights per datasource table
      val datasourceWeights = otc.map(otcDS => broadcast(otcDS.dataSources.toDS()).toDF
        .withColumnRenamed("id", "datasource_id")
        .select("datasource_id", "weight")
        .orderBy($"datasource_id".asc))

      val dtAssocs = datasourceWeights match {
        case Some(ws) =>
          df
            .join(ws, Seq("datasource_id"), "left_outer")
            // fill null for weight to default weight in case we have new datasources
            .na
            .fill(otc.get.defaultWeight, Seq("weight"))
        case None =>
          df.withColumn("weight", lit(1D))
      }

      val rankedScores = scoreColNames.foldLeft(dtAssocs)((b, name) => {
        val AintB = setA intersect setB
        val AunB = setA union setB

        //      val wds = Window.partitionBy(col("datasource_id"))
        //      val wt = Window.partitionBy(col("datasource_id"), col("target_id"))
        //      val wd = Window.partitionBy(col("datasource_id"), col("disease_id"))
        //      val wtd = Window.partitionBy(col("datasource_id"), col("target_id"), col("disease_id"))

        val Pall = Window.partitionBy(AintB.map(col(_)).toList:_*)
        val PA = Window.partitionBy(setA.map(col(_)).toList:_*)
        val PB = Window.partitionBy(setB.map(col(_)).toList:_*)
        val PAB = Window.partitionBy(AunB.map(col(_)).toList:_*)

        val bb = b
          .withColumn(idName + "_t_sum", sum("evidence_score").over(Pall))
          .withColumn(idName + "_t_uniq_reports_A", sum(col("evidence_score")).over(PA))
          .withColumn(idName + "_t_uniq_reports_B", sum(col("evidence_score")).over(PB))
          .withColumn("uniq_reports_d", sum(col("evidence_score")).over(wd))
          .withColumn("A", sum(col("evidence_score")).over(wtd))
          .withColumn("A_count", count(col("evidence_id")).over(wtd))
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
          .drop("datasource_score_llr_max",
            "uniq_reports_t",
            "uniq_reports_d",
            "A_count",
            "llr",
            "datasource_score_llr"
          )
          .withColumn(name + "_ths_k", row_number() over(w.orderBy(col(name).desc)))
          .withColumn(name + "_ths_dx", col(name) / (powCol(col(name + "_ths_k"), 2D) * maxHarmonicValue(10000, 2, 1D)))
          .withColumn(name + "_ths_t",
            sum(col(name + "_ths_dx")).over(w) )
          .withColumn(name + "_hs", col(name + "_ths_t") * col("weight"))

        // remove temporal cols
        val droppedCols = bb.columns.filter(_.startsWith(idName + "_t_"))
        bb.drop(droppedCols:_*)
      })

      rankedScores.drop("weight")
    }

    def harmonicOver(pairColNames: Seq[String], scoreColNames: Seq[String],
                     otc: Option[AssociationsSection],
                     keepScoreVector: Boolean = true): DataFrame = {
      // obtain weights per datasource table
      val datasourceWeights = otc.map(otcDS => broadcast(otcDS.dataSources.toDS()).toDF
        .withColumnRenamed("id", "datasource_id")
        .select("datasource_id", "weight")
        .orderBy($"datasource_id".asc))

      val dtAssocs = datasourceWeights match {
        case Some(ws) =>
          df
            .join(ws, Seq("datasource_id"), "left_outer")
            // fill null for weight to default weight in case we have new datasources
            .na
            .fill(otc.get.defaultWeight, Seq("weight"))
        case None =>
          df.withColumn("weight", lit(1D))
      }

      val rankedScores = scoreColNames.foldLeft(dtAssocs)((b, name) => {

        val w = Window
          .partitionBy(pairColNames.map(col(_)):_*)

        val bb = b.withColumn(name + "_ths_k", row_number() over(w.orderBy(col(name).desc)))
          .withColumn(name + "_ths_dx", col(name) / (powCol(col(name + "_ths_k"), 2D) * maxHarmonicValue(10000, 2, 1D)))
          .withColumn(name + "_ths_t",
            sum(col(name + "_ths_dx")).over(w) )
          .withColumn(name + "_hs", col(name + "_ths_t") * col("weight"))

        val r = if (keepScoreVector) {
          bb.withColumn(name + "_ths_st",
            struct(col("datasource_id"),
              col("weight"),
              col(name + "_ths_t").as(name + "_ths_raw")))
            .withColumn(name + "_dts", collect_set(col(name + "_ths_st")).over(w))
        } else {
          bb
        }

        // remove temporal cols
        val droppedCols = r.columns.filter(_.startsWith(name + "_ths"))
        r.drop(droppedCols:_*)
      })

      rankedScores.drop("weight")
    }

    def groupByDataSources(
        datasources: Dataset[DataSource],
        otc: AssociationsSection
    ): DataFrame = {
      val wds = Window.partitionBy(col("datasource_id"))
      val wt = Window.partitionBy(col("datasource_id"), col("target_id"))
      val wd = Window.partitionBy(col("datasource_id"), col("disease_id"))
      val wtd = Window.partitionBy(col("datasource_id"), col("target_id"), col("disease_id"))

      val datasourceAssocs = df
        .harmonicOver(Seq("datasource_id", "disease_id", "target_id"), Seq("evidence_score"), None, false)
        .withColumnRenamed("evidence_score_hs", "datasource_score_harmonic")
        .withColumn("datasource_evidence_count", count("evidence_id").over(wds))
        .withColumn("datasource_evidence_sum", sum("evidence_score").over(wds))
        .withColumn("uniq_reports_t", sum(col("evidence_score")).over(wt))
        .withColumn("uniq_reports_d", sum(col("evidence_score")).over(wd))
        .withColumn("A", sum(col("evidence_score")).over(wtd))
        .withColumn("A_count", count(col("evidence_id")).over(wtd))
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
          .drop("datasource_score_llr_max",
            "uniq_reports_t",
            "uniq_reports_d",
            "A_count",
            "llr",
            "datasource_score_llr"
          )

      datasourceAssocs
    }
  }
}

object Association extends LazyLogging {

  def prepareEvidences(expandOntology: Boolean = false)(implicit context: ETLSessionContext): DataFrame = {
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

    if (expandOntology) {
      val diseases = Disease.compute()

      dfs("evidences")
        .selectExpr(evidenceColumns:_*)
        .where($"evidence_score" > 0D)
        .computeOntologyExpansion(diseases, associationsSec)
        .repartitionByRange($"datasource".asc, $"disease_id".asc)
        .sortWithinPartitions($"datasource".asc, $"disease_id".asc)

    } else {
      dfs("evidences")
        .selectExpr(evidenceColumns:_*)
        .where($"evidence_score" > 0D)
        .repartitionByRange($"datasource".asc, $"disease_id".asc)
        .sortWithinPartitions($"datasource".asc, $"disease_id".asc)
    }

  }

  def computeDirectAssociations()(implicit context: ETLSessionContext): Map[String, DataFrame] = {
    implicit val ss = context.sparkSession
    import ss.implicits._

    val evidenceSet = prepareEvidences().persist()
    val associationsPerDS = computeAssociationsPerDS(evidenceSet).persist()
    val associationsOverall = computeAssociationsAllDS(associationsPerDS)

    Map(
      "associations_per_datasource_direct" -> associationsPerDS,
      "associations_overall_direct" -> associationsOverall
    )

  }

  def computeIndirectAssociations()(implicit context: ETLSessionContext): Map[String, DataFrame] = {
    implicit val ss = context.sparkSession
    import ss.implicits._

    val evidenceSet = prepareEvidences(true).persist()
    val associationsPerDS = computeAssociationsPerDS(evidenceSet).persist()
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

    val cols = Seq(
      "disease_id",
      "target_id",
      "overall_hs_score_from_llr",
      "dts_hs_score_from_llr",
      "overall_hs_score_from_harmonic",
      "dts_hs_score_from_harmonic"
    )
    import ss.implicits._
    import AssociationHelpers._

    assocsPerDS
      .harmonicOver(
        Seq("disease_id", "target_id"),
        Seq("datasource_score_llr_norm", "datasource_score_harmonic"),
        Some(associationsSec)
    )
      .withColumnRenamed("datasource_score_llr_norm_hs", "overall_hs_score_from_llr")
      .withColumnRenamed("datasource_score_llr_norm_dts", "dts_hs_score_from_llr")
      .withColumnRenamed("datasource_score_harmonic_hs", "overall_hs_score_from_harmonic")
      .withColumnRenamed("datasource_score_harmonic_dts", "dts_hs_score_from_harmonic")
      .selectExpr(cols:_*)
      .dropDuplicates("disease_id", "target_id")
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
      .dropDuplicates("datasource_id", "disease_id", "target_id")
      .repartitionByRange($"disease_id".asc)
      .sortWithinPartitions($"disease_id".asc)
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
