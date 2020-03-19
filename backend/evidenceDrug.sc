import $file.common
import common._

import org.apache.spark.SparkConf
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.typesafe.config.Config

object EvidenceDrugHelpers {
  implicit class AggregationHelpers(df: DataFrame)(implicit ss: SparkSession) {
    import Configuration._
    import ss.implicits._

    def getDiseaseAndDescendants: DataFrame = {

      val genAncestors = udf((codes: Seq[Seq[String]]) => codes.view.flatten.toSet.toSeq)

      val efos = df
        .withColumn("id", substring_index(col("code"), "/", -1))
        .withColumn("ancestors", genAncestors(col("path_codes")))
        .drop("paths", "private", "_private", "path")

      val descendants = efos
        .where(size(col("ancestors")) > 0)
        .withColumn("ancestor", explode(col("ancestors")))
        // all diseases have an ancestor, at least itself
        .groupBy("ancestor")
        .agg(collect_set(col("id")).as("descendants"))
        .withColumnRenamed("ancestor", "id")

      efos
        .join(descendants, Seq("id"))
        .drop("code", "children", "path_codes", "path_labels")
    }

    def generateEntries(dfEvidences: DataFrame): DataFrame = {

      val fds = dfEvidences
        .where(col("private.datatype") === "known_drug")
        .withColumn("disease_id", col("disease.id"))
        .withColumn("target_id", col("target.id"))
        .withColumn("drug_id", substring_index(col("drug.id"), "/", -1))
        .join(df, Seq("disease_id"), "inner")
        .withColumn("ancestor", explode(col("ancestors")))

      val associated = fds
        .groupBy(col("ancestor"), col("drug_id"))
        .agg(
          collect_set(col("disease_id")).as("associated_diseases"),
          collect_set(col("target_id")).as("associated_targets")
        )
        .withColumn("associated_targets_count", size(col("associated_targets")))
        .withColumn("associated_diseases_count", size(col("associated_diseases")))
        .withColumnRenamed("ancestor", "disease_id")

      val agg = fds
        .groupBy(
          col("ancestor"),
          col("drug_id"),
          col("evidence.drug2clinic.clinical_trial_phase.label").as("clinical_trial_phase"),
          col("evidence.drug2clinic.status").as("clinical_trial_status"),
          col("target_id").as("target")
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
        .withColumnRenamed("ancestor", "disease_id")
        .drop("_list_urls")
        .join(df, Seq("disease_id"), "inner")
        .withColumn("ancestors_count", size(col("ancestors")))
        .withColumn("descendants_count", size(col("descendants")))

      agg
        .join(broadcast(associated), Seq("disease_id", "drug_id"), "inner")
        .withColumnRenamed("disease_id", "disease")
        .withColumnRenamed("drug_id", "drug")

    }
  }
}

// This is option/step cancerbiomarkers in the config file
object EvidenceDrug extends LazyLogging {
  def apply(config: Config)(implicit ss: SparkSession) = {
    import ss.implicits._
    import EvidenceDrugHelpers._

    val common = Configuration.loadCommon(config)
    val mappedInputs = Map(
      "disease" -> Map(
        "format" -> common.inputs.disease.format,
        "path" -> common.inputs.disease.path
      ),
      "evidence" -> Map(
        "format" -> common.inputs.evidence.format,
        "path" -> common.inputs.evidence.path
      )
    )
    val inputDataFrame = SparkSessionWrapper.loader(mappedInputs)

    val diseases = inputDataFrame("disease").getDiseaseAndDescendants
      .selectExpr("id as disease_id", "ancestors", "descendants")

    val dfEvidencesDrug = diseases.generateEntries(inputDataFrame("evidence"))

    SparkSessionWrapper.save(dfEvidencesDrug, common.output + "/evidenceDrug")
  }
}
