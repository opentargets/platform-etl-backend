package io.opentargets.etl.backend

import org.apache.spark.SparkConf
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.typesafe.config.Config
import better.files._
import better.files.File._
import io.opentargets.etl.backend.spark.Helpers
import io.opentargets.etl.backend.spark.Helpers.{
  IOResourceConfig,
  IOResourceConfs,
  IOResources
}
object DiseaseHelpers {
  implicit class AggregationHelpers(df: DataFrame)(implicit ss: SparkSession) {
    import Configuration._
    import ss.implicits._

    def setIdAndSelectFromDiseases: DataFrame = {

      val efosSummary = df
        .withColumn(
          "ancestors",
          array_except(
            array_distinct(flatten(col("path_codes"))),
            array(col("id"))
          )
        )

      val descendants = efosSummary
        .where(size(col("ancestors")) > 0)
        .withColumn("ancestor", explode(concat(array(col("id")), col("ancestors"))))
        .groupBy("ancestor")
        .agg(collect_set(col("id")).as("descendants"))
        .withColumnRenamed("ancestor", "id")
        .withColumn(
          "descendants",
          array_except(
            col("descendants"),
            array(col("id"))
          )
        )

      val efos = efosSummary
        .join(descendants, Seq("id"), "left")

      val efosRenamed = efos
        .withColumnRenamed("label", "name")
        .withColumnRenamed("definition", "description")
        .withColumnRenamed("therapeutic_codes", "therapeuticAreas")
        .withColumnRenamed("obsolete_terms", "obsoleteTerms")
        .drop("path_codes",
              "definition_alternatives",
              "therapeutic_codes"
        )

      efosRenamed

    }
  }
}

object Disease extends LazyLogging {
  def compute()(implicit context: ETLSessionContext): DataFrame = {
    implicit val ss = context.sparkSession
    import ss.implicits._
    import DiseaseHelpers._

    val diseaseConfiguration = context.configuration.disease

    logger.info("Loading raw inputs for Disease step.")
    val mappedInputs = Map(
      "disease" -> diseaseConfiguration.efoOntology
    )

    val inputDataFrames = Helpers.readFrom(mappedInputs)

    val diseaseDF = inputDataFrames("disease").setIdAndSelectFromDiseases

    diseaseDF
  }

  def apply()(implicit context: ETLSessionContext) = {
    implicit val ss = context.sparkSession
    import ss.implicits._
    import DiseaseHelpers._

    logger.info("transform disease dataset")
    // compute is mandatory for running Connection.
    val diseaseDF = compute()

    val outputs = context.configuration.disease.outputs
    logger.info(s"write to ${context.configuration.common.output}/disease")
    val dataframesToSave: Map[String, (DataFrame, IOResourceConfig)] = Map(
      "disease" -> (diseaseDF, outputs.diseases)
    )

    val ioResources: IOResources = dataframesToSave mapValues (_._1)
    val saveConfigs: IOResourceConfs = dataframesToSave mapValues (_._2)

    Helpers.writeTo(saveConfigs, ioResources)

  }
}
