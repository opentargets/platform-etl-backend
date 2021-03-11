package io.opentargets.etl.backend

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{pow => powCol}
import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers.{IOResource, IOResources}
import io.opentargets.etl.backend.spark.{Helpers => H}
import org.apache.spark.sql.expressions._
import org.apache.spark.storage.StorageLevel

import scala.math.pow
import scala.util.Random

object Association extends LazyLogging {

  val weightId = "weight"
  val tId = "targetId"
  val dId = "diseaseId"
  val dsId = "datasourceId"
  val dtId = "datatypeId"
  val evScore = "evidenceScore"
  val dsIdScore = "datasourceHarmonicScore"
  val dtIdScore = "datatypeHarmonicScore"
  val overallDsIdScore = "overallDatasourceHarmonicScore"
  val overallDtIdScore = "overallDatatypeHarmonicScore"
  val overallDsIdVector = "overallDatasourceHarmonicVector"
  val overallDtIdVector = "overallDatatypeHarmonicVector"
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
          (e, i) => e / powCol(i, 2D)
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
            .selectExpr(s"id as $dsId", "propagate")
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
        require((setA intersect setB).nonEmpty,
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
      def leftJoinWeights(otc: AssociationsSection, weightColName: String): DataFrame = {
        // obtain weights per datasource table
        val datasourceWeights =
          broadcast(otc.dataSources.toDS()).toDF
            .withColumnRenamed("id", dsId)
            .select(dsId, weightColName)
            .orderBy(col(dsId).asc)

        df.join(datasourceWeights, Seq(dsId), "left_outer")
          // fill null for weight to default weight in case we have new datasources
          .na
          .fill(otc.defaultWeight, Seq(weightColName))
      }

      def harmonicOver(pairColNames: Seq[String],
                       scoreColName: String,
                       outputColName: String,
                       weightColName: Option[String]): DataFrame = {

        val tName = Random.alphanumeric.take(5).mkString("", "", "_")
        val w = Window.partitionBy(pairColNames.map(col): _*)
        val weightC = weightColName.map(col).getOrElse(lit(1D))

        val tDF = df
          .withColumn(tName + "_ths_k", row_number() over w.orderBy(col(scoreColName).desc))
          .withColumn(tName + "_ths_dx_max", typedLit(1D) / powCol(col(tName + "_ths_k"), 2D))
          .withColumn(tName + "_ths_dx", col(scoreColName) / powCol(col(tName + "_ths_k"), 2D))
          .withColumn(tName + "_ths_t", sum(col(tName + "_ths_dx")).over(w))
          .withColumn(tName + "_ths_t_max", sum(col(tName + "_ths_dx_max")).over(w))
          .withColumn(outputColName, col(tName + "_ths_t") * weightC / col(tName + "_ths_t_max"))

        // remove temporal cols
        tDF.drop(tDF.columns.filter(_.startsWith(tName)): _*)
      }

      def groupByDataSources(diseases: DataFrame, targets: DataFrame)(
          implicit context: ETLSessionContext): DataFrame = {
        val cols = Seq(
          dtId,
          dsId,
          dId,
          tId,
          dsIdScore,
          dtIdScore,
          dsEvsCount,
          dtEvsCount
        )

        val associationsSec = context.configuration.associations
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
          .leftJoinWeights(associationsSec, weightId)
          .harmonicOver(dsPartition, evScore, dsIdScore, None)
          .harmonicOver(dtPartition, evScore, dtIdScore, Some(weightId))
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
    implicit val ss: SparkSession = context.sparkSession
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
      s"score as $evScore",
      dtId,
      dsId,
      "id as evidenceId"
    )

    if (expandOntology) {
      evidences.data
        .selectExpr(evidenceColumns: _*)
        .where(col(evScore) > 0D)
        .computeOntologyExpansion(dfs("diseases").data, associationsSec)
        .repartitionByRange(col(dsId).asc, col(dId).asc)
        .sortWithinPartitions(col(dsId).asc, col(dId).asc)

    } else {
      evidences.data
        .selectExpr(evidenceColumns: _*)
        .where(col(evScore) > 0D)
        .repartitionByRange(col(dsId).asc, col(dId).asc)
        .sortWithinPartitions(col(dsId).asc, col(dId).asc)
    }

  }

  def computeDirectAssociations()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession

    val outputs = context.configuration.associations.outputs

    val evidenceSet = prepareEvidences().persist(StorageLevel.DISK_ONLY)
    val associationsPerDS = computeAssociationsPerDS(evidenceSet).persist()
    val associationsPerDT =
      associationsPerDS.drop(dsId, dsEvsCount, dsIdScore).dropDuplicates(tId, dId, dtId)
    val associationsOverall = computeAssociationsAllDS(associationsPerDS)

    Map(
      "directByDatasource" -> IOResource(associationsPerDS.drop(dtId, dtEvsCount, dtIdScore),
                                         outputs.directByDatasource),
      "directByDatatype" -> IOResource(associationsPerDT, outputs.directByDatatype),
      "directByOverall" -> IOResource(associationsOverall, outputs.directByOverall)
    )
  }

  def computeIndirectAssociations()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession

    val outputs = context.configuration.associations.outputs

    val evidenceSet = prepareEvidences(expandOntology = true).persist()
    val associationsPerDS = computeAssociationsPerDS(evidenceSet).persist()
    val associationsPerDT =
      associationsPerDS.drop(dsId, dsEvsCount, dsIdScore).dropDuplicates(tId, dId, dtId)
    val associationsOverall = computeAssociationsAllDS(associationsPerDS)

    Map(
      "indirectByDatasource" -> IOResource(associationsPerDS.drop(dtId, dtEvsCount, dtIdScore),
                                           outputs.indirectByDatasource),
      "indirectByDatatype" -> IOResource(associationsPerDT, outputs.indirectByDatatype),
      "indirectByOverall" -> IOResource(associationsOverall, outputs.indirectByOverall)
    )
  }

  def computeAssociationsAllDS(assocsPerDS: DataFrame)(
      implicit context: ETLSessionContext): DataFrame = {
    def sumVector(c1: String, c2: String, w: WindowSpec): Column = aggregate(
      transform(
        collect_set(
          struct(col(c1), col(c2))
        ).over(w),
        c => c.getField(c2)
      ),
      lit(0D),
      (acc, x) => acc + x
    )

    implicit val ss: SparkSession = context.sparkSession

    val associationsSec = context.configuration.associations

    val cols = Seq(
      dId,
      tId,
      "diseaseLabel",
      "targetName",
      "targetSymbol",
      overallDsIdScore,
      overallDtIdScore,
      overallDsIdVector,
      overallDtIdVector,
      overallDsEvsCount,
      overallDtEvsCount
    )

    import Helpers._

    val pairPartition = Seq(dId, tId)

    val w = Window.partitionBy(pairPartition.map(col): _*)
    assocsPerDS
      .leftJoinWeights(associationsSec, weightId)
      .harmonicOver(Seq(dId, tId), dsIdScore, overallDsIdScore, Some(weightId))
      .harmonicOver(Seq(dId, tId), dtIdScore, overallDtIdScore, None)
      .withColumn(overallDsEvsCount, sumVector(dsId, dsEvsCount, w))
      .withColumn(overallDtEvsCount, sumVector(dtId, dtEvsCount, w))
      .withColumn(overallDsIdVector,
                  collect_set(
                    struct(
                      col(dsId),
                      col(dsIdScore),
                      col(dsEvsCount),
                      col(weightId)
                    )
                  ).over(w))
      .withColumn(overallDtIdVector,
                  collect_set(
                    struct(
                      col(dtId),
                      col(dtIdScore),
                      col(dtEvsCount),
                      lit(1D).as(weightId)
                    )
                  ).over(w))
      .selectExpr(cols: _*)
      .dropDuplicates(dId, tId)
  }

  def computeAssociationsPerDS(evidences: DataFrame)(
      implicit context: ETLSessionContext): DataFrame = {
    implicit val ss: SparkSession = context.sparkSession

    import ss.implicits._
    import Helpers._

    val mappedInputs = Map(
      "targets" -> context.configuration.associations.inputs.targets,
      "diseases" -> context.configuration.associations.inputs.diseases
    )
    val dfs = H.readFrom(mappedInputs)

    val diseases = dfs("diseases").data
    val targets = dfs("targets").data

    evidences
      .groupByDataSources(diseases, targets)
      .repartitionByRange($"diseaseId".asc)
  }

  def apply()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession

    val directs = computeDirectAssociations()
    val indirects = computeIndirectAssociations()

    val outputs = directs ++ indirects

    H.writeTo(outputs)
  }
}
