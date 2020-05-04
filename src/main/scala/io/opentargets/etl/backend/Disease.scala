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

object DiseaseHelpers {
  implicit class AggregationHelpers(df: DataFrame)(implicit ss: SparkSession) {
    import Configuration._
    import ss.implicits._

    def setIdAndSelectFromDiseases: DataFrame = {

      val getParents = udf((codes: Seq[Seq[String]]) =>
        codes
          .flatMap(path => if (path.size < 2) None else Some(path.reverse(1)))
          .toSet
          .toSeq
      )
      //codes.withFilter(_.size > 1).flatMap(_.reverse(1)).toSet)

      val dfPhenotypeId = df
        .drop("type")
        .withColumn(
          "sourcePhenotypes",
          when(
            size(col("phenotypes")) > 0,
            expr(
              "transform(phenotypes, phRow -> named_struct('url', phRow.uri,'name',  phRow.label, 'disease', substring_index(phRow.uri, '/', -1)))"
            )
          )
        )

      //import ss.implicits._

      val efosSummary = dfPhenotypeId
        .withColumn("id", substring_index(col("code"), "/", -1))
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
        //.withColumn("test",when(size(col("sourcePhenotypes")) > 0, extractIdPhenothypes(col("sourcePhenotypes"))).otherwise(col("sourcePhenotypes")))
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
          expr("transform(children, child -> child.code)")
        )

      val descendants = efosSummary
        .where(size(col("ancestors")) > 0)
        .withColumn("ancestor", explode(col("ancestors")))
        // all diseases have an ancestor, at least itself
        .groupBy("ancestor")
        .agg(collect_set(col("id")).as("descendants"))
        .withColumnRenamed("ancestor", "id")

      val efos = efosSummary
        .join(descendants, Seq("id"), "left")
        .withColumn(
          "is_id_included_descendent",
          array_contains(col("descendants"), col("id"))
        )

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

  def generateEFOfile(efoBasicInfoDF: DataFrame, outputFilename: String) {

    val efoFile: File = File(outputFilename)
      .createIfNotExists(createParents = true)
      .append(efoBasicInfoDF.toJSON.collect.mkString("[", ",", "]"))
  }

  def generateTherapeticAreaFile(therapeticAreasList: Array[Any], outputFilename: String) {
    val therapeuticAreaFile: File = File(outputFilename)
      .createIfNotExists(createParents = true)
      .append(therapeticAreasList.mkString("[\"", "\",\"", "\"]"))
  }

  def apply(config: Config)(implicit ss: SparkSession) = {
    import ss.implicits._
    import DiseaseHelpers._

    val common = Configuration.loadCommon(config)
    val mappedInputs = Map(
      "disease" -> Map(
        "format" -> common.inputs.disease.format,
        "path" -> common.inputs.disease.path
      )
    )
    val inputDataFrame = SparkSessionWrapper.loader(mappedInputs)

    val diseaseDF = inputDataFrame("disease").setIdAndSelectFromDiseases

    SparkSessionWrapper.save(diseaseDF, common.output + "/diseases")

    val therapeticAreaList = diseaseDF
      .filter(col("ontology.isTherapeuticArea") === true)
      .select("id")
      .collect
      .map(_.toSeq)
      .flatten

    generateTherapeticAreaFile(therapeticAreaList, common.output + "/diseases_staticfiles")

    val efoBasicInfoDF =
      diseaseDF.select("id", "name", "parents").withColumnRenamed("parents", "parentIds")

    generateEFOfile(efoBasicInfoDF, common.output + "/diseases_staticfiles")

  }
}
