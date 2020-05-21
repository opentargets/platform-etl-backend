package io.opentargets.etl.backend

import org.apache.spark.SparkConf
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.typesafe.config.Config
import io.opentargets.etl.backend.SparkHelpers.IOResourceConfig

object EvidenceDrugDirectHelpers {
  implicit class AggregationHelpers(df: DataFrame)(implicit ss: SparkSession) {
    import Configuration._
    import ss.implicits._

    def generateEntries: DataFrame = {

      val fds = df
        .where(col("private.datatype") === "known_drug")
        .withColumn("disease_id", col("disease.id"))
        .withColumn("label", col("disease.efo_info.label"))
        .withColumn("target_id", col("target.id"))
        .withColumn("approvedSymbol", col("target.gene_info.symbol"))
        .withColumn("drug_id", substring_index(col("drug.id"), "/", -1))
        .withColumn("prefName", col("drug.molecule_name"))

      val dfDirect = fds
        .groupBy(
          col("disease_id").as("disease"),
          col("drug_id").as("drug"),
          col("evidence.drug2clinic.clinical_trial_phase.label").as("clinical_trial_phase"),
          col("evidence.drug2clinic.status").as("clinical_trial_status"),
          col("target_id").as("target"),
          col("prefName"),
          col("label"),
          col("approvedSymbol")
        )
        .agg(
          collect_list(col("evidence.drug2clinic.urls")).as("_list_urls"),
          count(col("evidence.drug2clinic.urls")).as("list_urls_counts"),
          first(col("drug.molecule_type")).as("drug_type"),
          first(col("evidence.target2drug.mechanism_of_action")).as("mechanism_of_action"),
          first(col("target.activity")).as("activity"),
          first(col("target.target_class")).as("target_class")
        )
        .withColumn("list_urls", flatten(col("_list_urls")))
        .drop("_list_urls")

      dfDirect
    }
  }
}

object EvidenceDrugDirect extends LazyLogging {
  def apply()(implicit context: ETLSessionContext) = {
    implicit val ss = context.sparkSession
    import ss.implicits._
    import EvidenceDrugDirectHelpers._

    val common = context.configuration.common
    val mappedInputs = Map(
      "evidence" -> IOResourceConfig(
        common.inputs.evidence.format,
        common.inputs.evidence.path
      )
    )
    val inputDataFrame = SparkHelpers.readFrom(mappedInputs)

    val dfDirectInfo = inputDataFrame("evidence").generateEntries

    val outputs = Seq("evidenceDrugDirect")

    // TODO THIS NEEDS MORE REFACTORING WORK AS IT CAN BE SIMPLIFIED
    val outputConfs = outputs
      .map(name =>
        name -> IOResourceConfig(
          context.configuration.common.outputFormat,
          context.configuration.common.output + s"/$name"
        )
      )
      .toMap

    val outputDFs = (outputs zip Seq(dfDirectInfo)).toMap
    SparkHelpers.writeTo(outputConfs, outputDFs)
  }
}
