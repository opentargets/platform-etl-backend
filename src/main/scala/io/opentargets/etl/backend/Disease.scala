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
import io.opentargets.etl.backend.spark.Helpers.{IOResourceConfig, stripIDFromURI}

object DiseaseHelpers {
  implicit class AggregationHelpers(df: DataFrame)(implicit ss: SparkSession) {
    import Configuration._
    import ss.implicits._

    def setIdAndSelectFromDiseases: DataFrame = {
      val getParents = udf((codes: Seq[Seq[String]]) =>
        codes
          .flatMap{
            path => if (path.size < 2) None else Some(path.reverse.slice(1, 2).head)
          }.distinct
      )

      val dfPhenotypeId = df
        .drop("type")
        .withColumn(
          "sourcePhenotypes",
          when(
            size(col("phenotypes")) > 0,
            transform($"phenotypes", c => {
              val idC = stripIDFromURI(c.getField("uri")).as("disease")
              val nameC = c.getField("label").as("name")
              val urlC = c.getField("uri").as("url")

              struct(urlC, nameC, idC)
            })
          )
        )

      val efosSummary = dfPhenotypeId
        .withColumn("id", stripIDFromURI(col("code")))
        .withColumn(
          "ancestors",
          array_except(
            array_distinct(flatten(col("path_codes"))),
            array(col("id"))
          )
        )
        .withColumn("parents", getParents(col("path_codes")))
        .drop("paths", "private", "_private", "path")
        .withColumn(
          "phenotypes",
          when(size(col("sourcePhenotypes")) > 0, struct(col("sourcePhenotypes").as("rows")))
            .otherwise(lit(null))
        )
        .withColumn("isTherapeuticArea", size(flatten(col("path_codes"))) === 1)
        .as("isTherapeuticArea")
        .withColumn("leaf", size(col("children")) === 0)
        .withColumn(
          "sources",
          struct(
            (col("code")).as("url"),
            col("id").as("name")
          )
        )
        .withColumn(
          "ontology",
          struct(
            (col("isTherapeuticArea")).as("isTherapeuticArea"),
            col("leaf").as("leaf"),
            col("sources").as("sources")
          )
        )
        // Change the value of children from array struct to array of code.
        .withColumn(
          "children",
          array_distinct(expr("transform(children, child -> child.code)"))
        )

      val descendants = efosSummary
        .where(size(col("ancestors")) > 0)
        .withColumn("ancestor",
          explode(concat(array(col("id")), col("ancestors"))))
        .groupBy("ancestor")
        .agg(collect_set(col("id")).as("descendants"))
        .withColumnRenamed("ancestor", "id")
        .withColumn("descendants",
          array_except(
            col("descendants"),
            array(col("id"))
          ))

      val efos = efosSummary
        .join(descendants, Seq("id"), "left")

      val efosRenamed = efos
        .withColumnRenamed("label", "name")
        .withColumnRenamed("definition", "description")
        .withColumnRenamed("efo_synonyms", "synonyms")
        .withColumnRenamed("therapeutic_codes", "therapeuticAreas")
        .drop(
          "definition_alternatives",
          "path_codes",
          "isTherapeuticArea",
          "leaf",
          "path_labels",
          "therapeutic_labels",
          "sources",
          "phenotypesCount",
          "sourcePhenotypes"
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

    val common = context.configuration.common
    val mappedInputs = Map(
      "disease" -> IOResourceConfig(
        common.inputs.disease.format,
        common.inputs.disease.path
      )
    )
    val inputDataFrame = Helpers.readFrom(mappedInputs)

    val diseaseDF = inputDataFrame("disease").setIdAndSelectFromDiseases

    diseaseDF
  }

  def apply()(implicit context: ETLSessionContext) = {
    implicit val ss = context.sparkSession
    import ss.implicits._
    import DiseaseHelpers._

    val common = context.configuration.common

    logger.info("transform disease dataset")
    val diseaseDF = compute()

    logger.info(s"write to ${context.configuration.common.output}/disease")
    val outputConfs = Map(
      "disease" -> IOResourceConfig(
        context.configuration.common.outputFormat,
        s"${context.configuration.common.output}/diseases"
      )
    )

    Helpers.writeTo(outputConfs, Map("disease" -> diseaseDF))

    val therapeticAreaList = diseaseDF
      .filter(col("ontology.isTherapeuticArea") === true)
      .select("id")

    therapeticAreaList
      .coalesce(1)
      .write
      .option("header", "false")
      .csv(common.output + "/diseasesStaticTherapeuticarea")

    val efoBasicInfoDF =
      diseaseDF.select("id", "name", "parents").withColumnRenamed("parents", "parentIds")

    efoBasicInfoDF
      .coalesce(1)
      .write
      .json(common.output + "/diseasesStaticEfos")
  }
}
