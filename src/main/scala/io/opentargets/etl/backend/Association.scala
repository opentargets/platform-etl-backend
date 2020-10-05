package io.opentargets.etl.backend

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{pow => powCol}
import com.typesafe.scalalogging.{LazyLogging, Logger}
import io.opentargets.etl.backend.SparkHelpers.IOResourceConfig
import org.apache.spark.sql.expressions._

import scala.math.pow

object AssociationHelpers extends LazyLogging {
  def maxHarmonicValue(vSize: Int, exp: Int, maxScore: Double): Double =
    (0 until vSize).foldLeft(0d)((acc: Double, n: Int) => acc + (maxScore / pow(1d + n, exp)))

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
        .selectExpr(diseaseCols: _*)

      // map datasource list to a dataset
      val datasources = broadcast(
        otc.dataSources
          .toDS()
          .selectExpr("id as datasource_id", "propagate")
          .orderBy($"datasource_id".asc))

      /*
       ontology propagation happens just when datasource is not one of the banned ones
       by configuration file application.conf when the known datasource is specified
       */
      val dfWithLut = df
        .join(datasources, Seq("datasource_id"), "left_outer")
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

    def llrOver(setA: Set[String],
                setB: Set[String],
                scoreColNames: Seq[String],
                prefixOutput: String,
                otc: Option[AssociationsSection]): DataFrame = {
      require((setA intersect setB) nonEmpty,
              logger.error("intersection column sets must be non empty"))

      // obtain weights per datasource table
      val datasourceWeights = otc.map(
        otcDS =>
          broadcast(otcDS.dataSources.toDS()).toDF
            .withColumnRenamed("id", "datasource_id")
            .select("datasource_id", "weight")
            .orderBy($"datasource_id".asc))

      val dtAssocs = datasourceWeights match {
        case Some(ws) =>
          df.join(ws, Seq("datasource_id"), "left_outer")
            // fill null for weight to default weight in case we have new datasources
            .na
            .fill(otc.get.defaultWeight, Seq("weight"))
        case None =>
          df.withColumn("weight", lit(1D))
      }

      val rankedScores = scoreColNames.foldLeft(dtAssocs)((b, name) => {
        val AintB = (setA intersect setB).map(col).toSeq
        val AunB = (setA union setB).map(col).toSeq
        val sA = setA.map(col).toSeq
        val sB = setB.map(col).toSeq

        val Pall = Window.partitionBy(AintB: _*)
        val PA = Window.partitionBy(sA: _*)
        val PB = Window.partitionBy(sB: _*)
        val PAB = Window.partitionBy(AunB: _*)

        val tName = prefixOutput + s"_${name}_t"

        val A = tName + "_t_A"
        val B = tName + "_t_B"
        val C = tName + "_t_C"
        val D = tName + "_t_D"
        val cA = col(A)
        val cB = col(B)
        val cC = col(C)
        val cD = col(D)

        val bb = b
          .withColumn(tName + "_t_sum", col(name) * col("weight"))
          .withColumn(tName + "_t_sum_w", sum(tName + "_t_sum").over(Pall))
          .withColumn(tName + "_t_uniq_reports_A", sum(tName + "_t_sum").over(PA))
          .withColumn(tName + "_t_uniq_reports_B", sum(tName + "_t_sum").over(PB))
          .withColumn(A, sum(tName + "_t_sum").over(PAB))
          .withColumn(C, col(tName + "_t_uniq_reports_B") - cA)
          .withColumn(B, col(tName + "_t_uniq_reports_A") - cA)
          .withColumn(
            D,
            col(tName + "_t_sum_w") -
              col(tName + "_t_uniq_reports_B") -
              col(tName + "_t_uniq_reports_A") +
              cA
          )
          .withColumn(tName + "_t_aterm", cA * (log(cA) - log(cA + cB)))
          .withColumn(tName + "_t_cterm", cC * (log(cC) - log(cC + cD)))
          .withColumn(tName + "_t_acterm", (cA + cC) * (log(cA + cC) - log(cA + cB + cC + cD)))
          .withColumn(tName + "_t_llr",
                      col(tName + "_t_aterm") + col(tName + "_t_cterm") - col(tName + "_t_acterm"))
          .withColumn(tName + "_t_llr_raw",
                      when(col(tName + "_t_llr").isNotNull and !col(tName + "_t_llr").isNaN,
                           col(tName + "_t_llr")).otherwise(lit(0d)))
          .withColumn(tName + "_t_llr_raw_max", max(tName + "_t_llr_raw").over(Pall))
          .withColumn(prefixOutput + s"${name}_score",
                      col(tName + "_t_llr_raw") / col(tName + "_t_llr_raw_max"))

        // remove temporal cols
        val droppedCols = bb.columns.filter(_.startsWith(tName))
        bb.drop(droppedCols: _*)
      })

      rankedScores.drop("weight")
    }

    def harmonicOver(pairColNames: Seq[String],
                     scoreColNames: Seq[String],
                     prefixOutput: String,
                     otc: Option[AssociationsSection],
                     keepScoreOverColumn: Option[String]): DataFrame = {
      // obtain weights per datasource table
      val datasourceWeights = otc.map(
        otcDS =>
          broadcast(otcDS.dataSources.toDS()).toDF
            .withColumnRenamed("id", "datasource_id")
            .select("datasource_id", "weight")
            .orderBy($"datasource_id".asc))

      val dtAssocs = datasourceWeights match {
        case Some(ws) =>
          df.join(ws, Seq("datasource_id"), "left_outer")
            // fill null for weight to default weight in case we have new datasources
            .na
            .fill(otc.get.defaultWeight, Seq("weight"))
        case None =>
          df.withColumn("weight", lit(1D))
      }

      val rankedScores = scoreColNames.foldLeft(dtAssocs)((b, name) => {

        val tName = prefixOutput + s"_${name}_t"

        val w = Window
          .partitionBy(pairColNames.map(col): _*)

        val bb = b
          .withColumn(tName + "_ths_k", row_number() over w.orderBy(col(name).desc))
          .withColumn(
            tName + "_ths_dx",
            col(name) / (powCol(col(tName + "_ths_k"), 2D) * maxHarmonicValue(10000, 2, 1D)))
          .withColumn(tName + "_ths_t", sum(col(tName + "_ths_dx")).over(w))
          .withColumn(prefixOutput + $"${name}_score", col(tName + "_ths_t") * col("weight"))

        val r = keepScoreOverColumn.foldLeft(bb)((b, colName) => {
          b.withColumn(tName + "_ths_st",
                        struct(col(colName),
                               col("weight"),
                               col(tName + "_ths_t").as(prefixOutput + $"${name}_score_raw")))
            .withColumn(prefixOutput + $"${name}_dts", collect_set(col(tName + "_ths_st")).over(w))
        })

        // remove temporal cols
        val droppedCols = r.columns.filter(_.startsWith(tName))
        r.drop(droppedCols: _*)
      })

      rankedScores.drop("weight")
    }

    def groupByDataSources(diseases: DataFrame, targets: DataFrame): DataFrame = {

      val cols = Seq(
        "datatype_id",
        "datasource_id",
        "disease_id",
        "target_id",
        "datasource_hs_evidence_score_score as datasource_harmonic",
        "datatype_hs_evidence_score_score as datatype_harmonic"
//         "datasource_llr_evidence_score_score as datasource_llr"
      )

      val ddf = broadcast(
        diseases
          .selectExpr("id as disease_id", "name as disease_label")
          .orderBy(col("disease_id")))

      val tdf = broadcast(
        targets
          .selectExpr("id as target_id",
                      "approvedName as target_name",
                      "approvedSymbol as target_symbol")
          .orderBy(col("target_id")))

      val datasourceAssocs = df
        .harmonicOver(Seq("datasource_id", "disease_id", "target_id"),
                      Seq("evidence_score"),
                      "datasource_hs_",
                      None,
                      None)
        .harmonicOver(Seq("datatype_id", "disease_id", "target_id"),
                      Seq("evidence_score"),
                      "datatype_hs_",
                      None,
                      None)
//         .llrOver(Set("datasource_id", "disease_id"), Set("datasource_id", "target_id"), Seq("evidence_score"), "datasource_llr_", None)

      datasourceAssocs
        .selectExpr(cols: _*)
        .dropDuplicates("datasource_id", "disease_id", "target_id")
        .join(ddf, Seq("disease_id"), "left_outer")
        .join(tdf, Seq("target_id"), "left_outer")
    }
  }
}

object Association extends LazyLogging {

  def prepareEvidences(expandOntology: Boolean = false)(
      implicit context: ETLSessionContext): DataFrame = {
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
        .selectExpr(evidenceColumns: _*)
//        .where($"evidence_score" > 0D)
        .computeOntologyExpansion(diseases, associationsSec)
        .repartitionByRange($"datasource_id".asc, $"disease_id".asc)
        .sortWithinPartitions($"datasource_id".asc, $"disease_id".asc)

    } else {
      dfs("evidences")
        .selectExpr(evidenceColumns: _*)
//        .where($"evidence_score" > 0D)
        .repartitionByRange($"datasource_id".asc, $"disease_id".asc)
        .sortWithinPartitions($"datasource_id".asc, $"disease_id".asc)
    }

  }

  def computeDirectAssociations()(implicit context: ETLSessionContext): Map[String, DataFrame] = {
    implicit val ss = context.sparkSession
    import ss.implicits._

    val evidenceSet = prepareEvidences().persist()
    val associationsPerDS = computeAssociationsPerDS(evidenceSet).persist()
    val associationsOverall = computeAssociationsAllDS(associationsPerDS)

    Map(
      "associationsDatasourceDirect" -> associationsPerDS,
      "associationsOverallDirect" -> associationsOverall
    )
  }

  def computeIndirectAssociations()(implicit context: ETLSessionContext): Map[String, DataFrame] = {
    implicit val ss = context.sparkSession
    import ss.implicits._

    val evidenceSet = prepareEvidences(true).persist()
    val associationsPerDS = computeAssociationsPerDS(evidenceSet).persist()
    val associationsOverall = computeAssociationsAllDS(associationsPerDS)

    Map(
      "associationsDatasourceIndirect" -> associationsPerDS,
      "associationsOverallIndirect" -> associationsOverall
    )
  }

  def computeAssociationsAllDS(assocsPerDS: DataFrame)(
      implicit context: ETLSessionContext): DataFrame = {
    implicit val ss = context.sparkSession

    val associationsSec = context.configuration.associations
    val commonSec = context.configuration.common

    val cols = Seq(
      "disease_id",
      "target_id",
      "disease_label",
      "target_name",
      "target_symbol",
      "overall_hs_datasource_harmonic_score as overall_ds_score_harmonic",
      "overall_hs_datasource_harmonic_dts as overall_ds_score_harmonic_dts",
      "overall_hs_datatype_harmonic_score as overall_dt_score_harmonic",
      "overall_hs_datatype_harmonic_dts as overall_dt_score_harmonic_dts",
      "overall_hs_datatype_datasource_harmonic_score as overall_dtds_score_harmonic",
      "overall_hs_datatype_datasource_harmonic_dts as overall_dtds_score_harmonic_dts"
    )
    import ss.implicits._
    import AssociationHelpers._

    assocsPerDS
      .harmonicOver(Seq("datatype_id", "disease_id", "target_id"),
                    Seq("datasource_harmonic"),
                    "datatype_hs_",
                    None,
                    None)
      .withColumnRenamed("datatype_hs_datasource_harmonic_score", "datatype_datasource_harmonic")
      .harmonicOver(Seq("disease_id", "target_id"),
                    Seq("datasource_harmonic"),
                    "overall_hs_",
                    Some(associationsSec),
                    Some("datasource_id"))
      .harmonicOver(Seq("disease_id", "target_id"),
                    Seq("datatype_harmonic"),
                    "overall_hs_",
                    Some(associationsSec),
                    Some("datatype_id"))
      .harmonicOver(Seq("disease_id", "target_id"),
                    Seq("datatype_datasource_harmonic"),
                    "overall_hs_",
                    Some(associationsSec),
                    Some("datatype_id"))
      .selectExpr(cols: _*)
      .dropDuplicates("disease_id", "target_id")
  }

  def computeAssociationsPerDS(evidences: DataFrame)(
      implicit context: ETLSessionContext): DataFrame = {
    implicit val ss = context.sparkSession

    import ss.implicits._
    import AssociationHelpers._

    val diseases = Disease.compute()
    val targets = Target.compute()

    evidences
      .groupByDataSources(diseases, targets)
      .repartitionByRange($"disease_id".asc)
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
