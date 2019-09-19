import $ivy.`org.apache.spark::spark-core:2.4.3`
import $ivy.`org.apache.spark::spark-sql:2.4.3`
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

/**
  * install sdkman and scala 2.11.12
  * install ammonite repl
  * export some jvm mem things like this export JAVA_OPTS="-Xms1G -Xmx80G"
  * execute it as amm script.sc
  * paths are hardcoded!
  */
object Loaders {
  def loadDiseases(path: String)(implicit ss: SparkSession): DataFrame = {
    val genAncestors = udf((codes: Seq[Seq[String]]) =>
      codes.view.flatten.toSet.toSeq)

   val efos = ss.read.json(path)
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

    efos.join(descendants, Seq("id"))
      .drop("code", "children", "path_codes", "path_labels")
  }

  def loadEvidences(path: String)(implicit ss: SparkSession): DataFrame = {
    val evidences = ss.read.json(path)
    evidences
  }
}

@main
def main(evidencePath: String, efoPath: String, outPath: String): Unit = {
  val sparkConf = new SparkConf()
    .setAppName("drugs-aggregation")
    .setMaster("local[*]")

  implicit val ss = SparkSession.builder
    .config(sparkConf)
    .getOrCreate

  val ddf = Loaders.loadEvidences(evidencePath)
  val efos = Loaders.loadDiseases(efoPath)
    .selectExpr("id as disease_id", "ancestors", "descendants")

  val agg = ddf
    .where(col("private.datatype") === "known_drug")
    .withColumn("disease_id", col("disease.id"))
    .withColumn("drug_id", substring_index(col("drug.id"), "/", -1))
    .join(efos, Seq("disease_id"), "inner")
    .withColumn("ancestor", explode(col("ancestors")))
    .groupBy(col("ancestor"),
      col("drug_id"),
      col("evidence.drug2clinic.clinical_trial_phase.label").as("clinical_trial_phase"),
      col("evidence.drug2clinic.status").as("clinical_trial_status"),
      col("target.target_name").as("target_name"))
    .agg(collect_list(col("evidence.drug2clinic.urls")).as("_list_urls"),
      count(col("evidence.drug2clinic.urls")).as("list_urls_counts"),
      first(col("drug.molecule_type")).as("drug_type"),
      first(col("evidence.target2drug.mechanism_of_action")).as("mechanism_of_action"),
      first(col("target.activity")).as("activity"),
      first(col("target.target_class")).as("target_class")
    )
      .withColumn("list_urls", flatten(col("_list_urls")))
      .withColumnRenamed("ancestor", "disease_id")
      .drop("_list_urls")
      .join(efos, Seq("disease_id"), "inner")
      .withColumn("ancestors_count", size(col("ancestors")))
      .withColumn("descendants_count", size(col("descendants")))

  agg.write
    .json(outPath)
}
