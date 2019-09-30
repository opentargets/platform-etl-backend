import $ivy.`com.typesafe:config:1.3.4`
import $ivy.`com.github.fommil.netlib:all:1.1.2`
import $ivy.`org.apache.spark::spark-core:2.4.3`
import $ivy.`org.apache.spark::spark-mllib:2.4.3`
import $ivy.`org.apache.spark::spark-sql:2.4.3`
import $ivy.`com.github.pathikrit::better-files:3.8.0`
import $ivy.`sh.almond::ammonite-spark:0.7.0`
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Loaders {

  def loadDiseases(path: String)(implicit ss: SparkSession): DataFrame = {
    val diseaseList = ss.read.json(path)
    diseaseList
  }

  def loadEvidences(path: String)(implicit ss: SparkSession): DataFrame = {
    val evidences = ss.read.json(path)
    evidences
  }
}

object Transformers {
  implicit class Implicits (val df: DataFrame) {

    def setIdAndSelectFromDiseases: DataFrame = {
      val genAncestors = udf((codes: Seq[Seq[String]]) =>
        codes.view.flatten.toSet.toSeq)

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


      efos.join(descendants, Seq("id"))
         .drop("code", "children", "path_codes", "path_labels")

      val lookup = Map("label" -> "name", "definition" -> "description", "efo_synonyms" -> "synonyms")
      efos.select(efos.columns.map(c => col(c).as(lookup.getOrElse(c, c))): _*)

    }

  }
}

@main
def main(diseaseFilename: String,
         evidenceFilename: String, outputPathPrefix: String): Unit = {
  val sparkConf = new SparkConf()
    .set("spark.driver.maxResultSize", "0")
    .setAppName("similarities-loaders")
    .setMaster("local[*]")

  implicit val ss = SparkSession.builder
    .config(sparkConf)
    .getOrCreate

  // AmmoniteSparkSession.sync()

  import ss.implicits._
  import Transformers.Implicits

  val diseases = Loaders.loadDiseases(diseaseFilename)
  val evidences = Loaders.loadEvidences(evidenceFilename)

  diseases
    .setIdAndSelectFromDiseases
    .write.json(outputPathPrefix + "/diseases/")

}
