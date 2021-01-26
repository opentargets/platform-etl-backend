package io.opentargets.etl.backend

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{pow => powCol}
import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers.IOResourceConfig
import io.opentargets.etl.backend.spark.{Helpers => H}
import org.apache.spark.sql.expressions._
import org.apache.spark.storage.StorageLevel

import scala.math.pow

object Association extends LazyLogging {

  val tId = "targetId"
  val dId = "diseaseId"
  val dsId = "datasourceId"
  val dtId = "datatypeId"
  val evScore = "evidenceScore"
  val dsIdScore = "datasourceHarmonicScore"
  val dtIdScore = "datatypeHarmonicScore"
  val overallDsIdScore = "overallDatasourceHarmonicScore"
  val overallDtIdScore = "overallDatatypeHarmonicScore"
  val dsEvsCount = "datasourceEvidenceCount"
  val dtEvsCount = "datatypeEvidenceCount"
  val overallDsEvsCount = "overallDatasourceEvidenceCount"
  val overallDtEvsCount = "overallDatatypeEvidenceCount"

  object Helpers extends LazyLogging {
    def maxHarmonicValue(vSize: Int, exp: Int, maxScore: Double): Double =
      (0 until vSize).foldLeft(0d)((acc: Double, n: Int) => acc + (maxScore / pow(1d + n, exp)))

    def maxHarmonicValueExpr(vsizeCol: String): Column =
      aggregate(
        zip_with(
          array_repeat(lit(1.0), size(col(vsizeCol))),
          sequence(lit(1), size(col(vsizeCol))),
          (e, i) => (e / powCol(i, 2D))
        ),
        lit(0D),
        (a, el) => a + el
      )

    implicit class ImplicitExtras(df: DataFrame)(implicit ss: SparkSession) {

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
            .selectExpr(s"id as ${dsId}", "propagate")
            .orderBy(col(dsId).asc))

        /*
         ontology propagation happens just when datasource is not one of the banned ones
         by configuration file application.conf when the known datasource is specified
         */
        val dfWithLut = df
          .join(datasources, Seq(dsId), "left_outer")
          .na
          .fill(otc.defaultPropagate, Seq("propagate"))
          .repartitionByRange(col(dId).asc)

        val fullExpanded = dfWithLut
          .join(broadcast(lut.orderBy($"did".asc)), col(dId) === $"did", "inner")
          .withColumn("_ancestors",
                      when($"propagate" === true, concat(array($"did"), $"ancestors"))
                        .otherwise(array($"did")))
          .withColumn("ancestor", explode($"_ancestors"))
          .drop(dId, "did", "ancestors", "_ancestors")
          .withColumnRenamed("ancestor", dId)

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
              .withColumnRenamed("id", dsId)
              .select(dsId, "weight")
              .orderBy(col(dsId).asc))

        val dtAssocs = datasourceWeights match {
          case Some(ws) =>
            df.join(ws, Seq(dsId), "left_outer")
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
            .withColumn(
              tName + "_t_llr",
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

      /**
        * join weight per datasource from configuration section `otc`
        * @param otc from ETL configuration section
        * @return the modified dataframe
        */
      def leftJoinWeights(otc: AssociationsSection): DataFrame = {
        val wC = "weight"
        // obtain weights per datasource table
        val datasourceWeights =
          broadcast(otc.dataSources.toDS()).toDF
            .withColumnRenamed("id", dsId)
            .select(dsId, wC)
            .orderBy(col(dsId).asc)

        df.join(datasourceWeights, Seq(dsId), "left_outer")
          // fill null for weight to default weight in case we have new datasources
          .na
          .fill(otc.defaultWeight, Seq(wC))
      }

      // TODO REMOVE COLLECTION AND JUST ONE PARAM OUT
      def harmonicOver(pairColNames: Seq[String],
                       scoreColNames: Seq[String],
                       prefixOutput: String,
                       keepScoreOverColumn: Option[String]): DataFrame = {

        val rankedScores = scoreColNames.foldLeft(dtAssocs)((b, name) => {

          val tName = prefixOutput + s"_${name}_t"

          val w = Window
            .partitionBy(pairColNames.map(col): _*)

          val bb = b
            .withColumn(tName + "_ths_k", row_number() over w.orderBy(col(name).desc))
            .withColumn(
              tName + "_ths_dx",
              col(name) / (powCol(col(tName + "_ths_k"), 2D) * maxHarmonicValue(100000, 2, 1D)))
            .withColumn(tName + "_ths_t", sum(col(tName + "_ths_dx")).over(w))
            .withColumn(prefixOutput + $"${name}_score", col(tName + "_ths_t") * col("weight"))

          // remove temporal cols
          val droppedCols = r.columns.filter(_.startsWith(tName))
          r.drop(droppedCols: _*)
        })

        rankedScores.drop("weight")
      }

      def groupByDataSources(diseases: DataFrame, targets: DataFrame): DataFrame = {
        val cols = Seq(
          dtId,
          dsId,
          dId,
          tId,
          s"datasource_hs_${evScore}_score as ${dsIdScore}",
          s"datatype_hs_${evScore}_score as ${dtIdScore}",
          dsEvsCount,
          dtEvsCount
        )

        val ddf = broadcast(
          diseases
            .selectExpr(s"id as $dId", "name as diseaseLabel")
            .orderBy(col(dId)))

        val tdf = broadcast(targets
          .selectExpr(s"id as $tId", "approvedName as targetName", "approvedSymbol as targetSymbol")
          .orderBy(col(tId)))

        val dsPartition = Seq(dsId, dId, tId)
        val dtPartition = Seq(dtId, dId, tId)

        val datasourceAssocs = df
          .harmonicOver(dsPartition, Seq(evScore), "datasource_hs_", None)
          .harmonicOver(dtPartition, Seq(evScore), "datatype_hs_", None)
          .withColumn(dsEvsCount,
                      count(expr("*")).over(Window.partitionBy(dsPartition.map(col): _*)))
          .withColumn(dtEvsCount,
                      count(expr("*")).over(Window.partitionBy(dtPartition.map(col): _*)))

        datasourceAssocs
          .selectExpr(cols: _*)
          .dropDuplicates(dsId, dId, tId)
          .join(ddf, Seq(dId), "left_outer")
          .join(tdf, Seq(tId), "left_outer")
      }
    }

  }

  def prepareEvidences(expandOntology: Boolean = false)(
      implicit context: ETLSessionContext): DataFrame = {
    implicit val ss = context.sparkSession
    import ss.implicits._
    import Helpers.ImplicitExtras

    val associationsSec = context.configuration.associations

    val mappedInputs = Map(
      "evidences" -> context.configuration.associations.inputs.evidences,
      "diseases" -> context.configuration.associations.inputs.diseases
    )
    val dfs = H.readFrom(mappedInputs)
    val evidences = dfs("evidences")

    val evidenceColumns = Seq(
      dId,
      tId,
      s"score as ${evScore}",
      dtId,
      dsId,
      "id as evidenceId"
    )

    if (expandOntology) {
      evidences
        .selectExpr(evidenceColumns: _*)
        .where(col(evScore) > 0D)
        .computeOntologyExpansion(dfs("diseases"), associationsSec)
        .repartitionByRange(col(dsId).asc, col(dId).asc)
        .sortWithinPartitions(col(dsId).asc, col(dId).asc)

    } else {
      evidences
        .selectExpr(evidenceColumns: _*)
        .where(col(evScore) > 0D)
        .repartitionByRange(col(dsId).asc, col(dId).asc)
        .sortWithinPartitions(col(dsId).asc, col(dId).asc)
    }

  }

  def computeDirectAssociations()(
      implicit context: ETLSessionContext): Map[String, (DataFrame, IOResourceConfig)] = {
    implicit val ss = context.sparkSession
    import ss.implicits._

    val outputs = context.configuration.associations.outputs

    val evidenceSet = prepareEvidences().persist(StorageLevel.DISK_ONLY)
    val associationsPerDS = computeAssociationsPerDS(evidenceSet).persist()
    val associationsOverall = computeAssociationsAllDS(associationsPerDS)

    Map(
      "directByDatasource" -> (associationsPerDS, outputs.directByDatasource),
      "directByOverall" -> (associationsOverall, outputs.directByOverall)
    )
  }

  def computeIndirectAssociations()(
      implicit context: ETLSessionContext): Map[String, (DataFrame, IOResourceConfig)] = {
    implicit val ss = context.sparkSession
    import ss.implicits._

    val outputs = context.configuration.associations.outputs

    val evidenceSet = prepareEvidences(true).persist()
    val associationsPerDS = computeAssociationsPerDS(evidenceSet).persist()
    val associationsOverall = computeAssociationsAllDS(associationsPerDS)

    Map(
      "indirectByDatasource" -> (associationsPerDS, outputs.indirectByDatasource),
      "indirectByOverall" -> (associationsOverall, outputs.indirectByOverall)
    )
  }

  def computeAssociationsAllDS(assocsPerDS: DataFrame)(
      implicit context: ETLSessionContext): DataFrame = {
    implicit val ss = context.sparkSession

    val associationsSec = context.configuration.associations

    val cols = Seq(
      dId,
      tId,
      "diseaseLabel",
      "targetName",
      "targetSymbol",
      s"overall_hs_${dsIdScore}_score as ${overallDsIdScore}",
      s"overall_hs_${dsIdScore}_dts as ${overallDsIdScore}DSs",
      s"overall_hs_${dtIdScore}_score as ${overallDtIdScore}",
      s"overall_hs_${dtIdScore}_dts as ${overallDtIdScore}DTs",
      overallDsEvsCount,
      overallDtEvsCount
    )

    import Helpers._

    val pairPartition = Seq(dId, tId)

    assocsPerDS
      .leftJoinWeights(associationsSec)
      .harmonicOver(Seq(dId, tId), Seq(dsIdScore), "overall_hs_", Some(dsId))
      .harmonicOver(Seq(dId, tId), Seq(dtIdScore), "overall_hs_", Some(dtId))
      .withColumn(overallDsEvsCount,
                  sum(col(dsEvsCount)).over(Window.partitionBy(pairPartition.map(col): _*)))
      .withColumn(overallDtEvsCount,
                  sum(col(dtEvsCount)).over(Window.partitionBy(pairPartition.map(col): _*)))

    // TODO ADD COLLECT LIST PER DATASOURCE AND PER DATATYPE
      .withColumn(overallDsEvsCount,
        sum(col(dsEvsCount)).over(Window.partitionBy(pairPartition.map(col): _*)))
      .withColumn(overallDtEvsCount,
        sum(col(dtEvsCount)).over(Window.partitionBy(pairPartition.map(col): _*)))
      .selectExpr(cols: _*)
      .dropDuplicates(dId, tId)
  }

  def computeAssociationsPerDS(evidences: DataFrame)(
      implicit context: ETLSessionContext): DataFrame = {
    implicit val ss = context.sparkSession

    import ss.implicits._
    import Helpers._

    val mappedInputs = Map(
      "targets" -> context.configuration.associations.inputs.targets,
      "diseases" -> context.configuration.associations.inputs.diseases
    )
    val dfs = H.readFrom(mappedInputs)

    val diseases = dfs("diseases")
    val targets = dfs("targets")

    evidences
      .groupByDataSources(diseases, targets)
      .repartitionByRange($"diseaseId".asc)
  }

  def apply()(implicit context: ETLSessionContext) = {
    implicit val ss = context.sparkSession
    val commonSec = context.configuration.common

    val directs = computeDirectAssociations()
    val indirects = computeIndirectAssociations()

    val outputDFs = directs ++ indirects

    val outputs = outputDFs map (p => p._1 -> p._2._2)
    val outputsData = outputDFs map (p => p._1 -> p._2._1)

    H.writeTo(outputs, outputsData)
  }
}
