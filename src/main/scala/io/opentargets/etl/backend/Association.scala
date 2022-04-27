package io.opentargets.etl.backend

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{pow => powCol}
import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers.mkRandomPrefix
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import org.apache.spark.sql.expressions._
import org.apache.spark.storage.StorageLevel

import scala.math.pow

object Association extends LazyLogging {
  import Helpers.maxHarmonicValue

  val weightId = "weight"
  val tId = "targetId"
  val dId = "diseaseId"
  val dsId = "datasourceId"
  val dtId = "datatypeId"
  val evScore = "evidenceScore"
  val evId = "evidenceId"
  val dsIdScore = "score"
  val dtIdScore = "score"
  val overallDsIdScore = "score"
  val overallDtIdScore = "score"
  val dsEvsCount = "evidenceCount"
  val dtEvsCount = "evidenceCount"
  val overallDsEvsCount = "evidenceCount"
  val overallDtEvsCount = "evidenceCount"
  val maxHS: Double = maxHarmonicValue(100000, 2, 1)

  object Helpers extends LazyLogging {
    def maxHarmonicValue(vSize: Int, exp: Int, maxScore: Double): Double =
      (0 until vSize).foldLeft(0d)((acc: Double, n: Int) => acc + (maxScore / pow(1d + n, exp)))

    def maxHarmonicValueExpr(vector: Column): Column =
      aggregate(
        zip_with(
          array_repeat(lit(1.0), size(vector)),
          sequence(lit(1), size(vector)),
          (e, i) => e / powCol(i, 2d)
        ),
        lit(0d),
        (a, el) => a + el
      )

    def harmonicFn(
        df: DataFrame,
        pairColNames: Seq[String],
        scoreColName: String,
        outputColName: String,
        weightColName: Option[String],
        scaler: Option[Column]
    ): DataFrame = {

      val tName = mkRandomPrefix()
      val w = Window.partitionBy(pairColNames.map(col): _*)
      val weightC = weightColName.map(col).getOrElse(lit(1d))
      val wScore = tName + "_ths_w"
      val tDF = df
        .withColumn(wScore, col(scoreColName) * weightC)
        .withColumn(tName + "_ths_k", row_number() over w.orderBy(col(wScore).desc))
        .withColumn(tName + "_ths_dx_max", typedLit(1d) / powCol(col(tName + "_ths_k"), 2d))
        .withColumn(tName + "_ths_dx", col(wScore) / powCol(col(tName + "_ths_k"), 2d))
        .withColumn(tName + "_ths_t", sum(col(tName + "_ths_dx")).over(w))
        .withColumn(tName + "_ths_t_max", sum(col(tName + "_ths_dx_max")).over(w))
        .withColumn(
          outputColName,
          col(tName + "_ths_t") / scaler.getOrElse(col(tName + "_ths_t_max"))
        )

      // remove temporal cols
      tDF.drop(tDF.columns.filter(_.startsWith(tName)): _*)
    }

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
            .orderBy(col(dsId).asc)
        )

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
          .withColumn(
            "_ancestors",
            when($"propagate" === true, concat(array($"did"), $"ancestors"))
              .otherwise(array($"did"))
          )
          .withColumn("ancestor", explode($"_ancestors"))
          .drop(dId, "did", "ancestors", "_ancestors")
          .withColumnRenamed("ancestor", dId)

        fullExpanded
      }

      /** join weight per datasource from configuration section `otc`
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
          .na
          .fill(otc.defaultWeight, Seq(weightColName))
      }

      def groupByDataSources(implicit context: ETLSessionContext): DataFrame = {
        val cols = Seq(
          dtId,
          dsId,
          dId,
          tId,
          dsIdScore,
          dsEvsCount
        )

        val associationsSec = context.configuration.associations
        val dsPartition = Seq(dsId, dId, tId)

        val datasourceAssocs = df
          // .leftJoinWeights(associationsSec, weightId)
          .transform(harmonicFn(_, dsPartition, evScore, dsIdScore, None, Some(lit(maxHS))))
          .withColumn(
            dsEvsCount,
            count(col(evId)).over(Window.partitionBy(dsPartition.map(col): _*))
          )

        val res = datasourceAssocs
          .selectExpr(cols: _*)
          .dropDuplicates(dsId, dId, tId)

        res
      }
    }

  }

  def prepareEvidences(
      expandOntology: Boolean = false
  )(implicit context: ETLSessionContext): DataFrame = {
    implicit val ss: SparkSession = context.sparkSession
    import Helpers.ImplicitExtras

    val associationsSec = context.configuration.associations

    val mappedInputs = Map(
      "evidences" -> context.configuration.associations.inputs.evidences,
      "diseases" -> context.configuration.associations.inputs.diseases
    )

    val evidenceColumns = Seq(
      dId,
      tId,
      s"score as $evScore",
      dtId,
      dsId,
      s"id as $evId"
    )

    val dfs = IoHelpers.readFrom(mappedInputs)
    val evidences = dfs("evidences").data
      .selectExpr(evidenceColumns: _*)
      .where(col(evScore) > 0d)

    val evs = if (expandOntology) {
      evidences
        .computeOntologyExpansion(dfs("diseases").data, associationsSec)
        .repartitionByRange(col(dsId).asc, col(dId).asc)
        .sortWithinPartitions(col(dsId).asc, col(dId).asc)

    } else {
      evidences
        .repartitionByRange(col(dsId).asc, col(dId).asc)
        .sortWithinPartitions(col(dsId).asc, col(dId).asc)
    }

    evs
  }

  def computeDirectAssociations()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession

    val outputs = context.configuration.associations.outputs

    val evidenceSet = prepareEvidences().persist(StorageLevel.DISK_ONLY)
    val associationsPerDS = computeAssociationsPerDS(evidenceSet).persist()
    val associationsPerDT = computeAssociationsPerDT(associationsPerDS)
    val associationsOverall = computeAssociationsAllDS(associationsPerDS)

    Map(
      "directByDatasource" -> IOResource(associationsPerDS, outputs.directByDatasource),
      "directByDatatype" -> IOResource(associationsPerDT, outputs.directByDatatype),
      "directByOverall" -> IOResource(associationsOverall, outputs.directByOverall)
    )
  }

  def computeIndirectAssociations()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession

    val outputs = context.configuration.associations.outputs

    val evidenceSet = prepareEvidences(expandOntology = true).persist()
    val associationsPerDS = computeAssociationsPerDS(evidenceSet).persist()
    val associationsPerDT = computeAssociationsPerDT(associationsPerDS)
    val associationsOverall = computeAssociationsAllDS(associationsPerDS)

    Map(
      "indirectByDatasource" -> IOResource(associationsPerDS, outputs.indirectByDatasource),
      "indirectByDatatype" -> IOResource(associationsPerDT, outputs.indirectByDatatype),
      "indirectByOverall" -> IOResource(associationsOverall, outputs.indirectByOverall)
    )
  }

  def computeAssociationsPerDT(
      assocsPerDS: DataFrame
  )(implicit context: ETLSessionContext): DataFrame = {

    implicit val ss: SparkSession = context.sparkSession

    val cols = Seq(
      dId,
      tId,
      dtId,
      dtIdScore,
      dtEvsCount
    )

    import Helpers._

    val pairPartition = Seq(dId, tId, dtId)

    val w = Window.partitionBy(pairPartition.map(col): _*)

    val dsScoreName = mkRandomPrefix() + "DSScore"
    val dsCountName = mkRandomPrefix() + "DSCount"

    val res = assocsPerDS
      .withColumnRenamed(dsIdScore, dsScoreName)
      .withColumnRenamed(dsEvsCount, dsCountName)
      .leftJoinWeights(context.configuration.associations, weightId)
      .transform(harmonicFn(_, pairPartition, dsScoreName, overallDtIdScore, None, None))
      .withColumn(dtEvsCount, sum(col(dsCountName)).over(w))
      .selectExpr(cols: _*)
      .dropDuplicates(pairPartition)

    res
  }

  def computeAssociationsAllDS(
      assocsPerDS: DataFrame
  )(implicit context: ETLSessionContext): DataFrame = {
    implicit val ss: SparkSession = context.sparkSession
    import Helpers._

    val cols = Seq(
      dId,
      tId,
      overallDsIdScore,
      overallDsEvsCount
    )

    val pairPartition = Seq(dId, tId)
    val w = Window.partitionBy(pairPartition.map(col): _*)
    val dsScoreName = mkRandomPrefix() + "DSScore"
    val dsCountName = mkRandomPrefix() + "DSCount"

    val res = assocsPerDS
      .leftJoinWeights(context.configuration.associations, weightId)
      .withColumnRenamed(dsIdScore, dsScoreName)
      .withColumnRenamed(dsEvsCount, dsCountName)
      .transform(
        harmonicFn(
          _,
          pairPartition,
          dsScoreName,
          overallDsIdScore,
          Some(weightId),
          Some(lit(maxHS))
        )
      )
      .withColumn(overallDsEvsCount, sum(col(dsCountName)).over(w))
      .selectExpr(cols: _*)
      .dropDuplicates(pairPartition)

    res
  }

  def computeAssociationsPerDS(
      evidences: DataFrame
  )(implicit context: ETLSessionContext): DataFrame = {
    implicit val ss: SparkSession = context.sparkSession

    import ss.implicits._
    import Helpers._

    evidences.groupByDataSources
      .repartitionByRange($"diseaseId".asc)
  }

  def apply()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession

    val directs = computeDirectAssociations()
    val indirects = computeIndirectAssociations()

    val outputs = directs ++ indirects

    IoHelpers.writeTo(outputs)
  }
}
