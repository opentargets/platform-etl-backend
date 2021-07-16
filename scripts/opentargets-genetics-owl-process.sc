// how to get the vectors, operate and find synonyms
// val vectors = model.getVectors
//  .filter($"word" isInCollection(Seq(pi3k, atk1, "ENSG00000105221", "ENSG00000140992", "ENSG00000152256")))
//  .agg(Summarizer.sum($"vector").as("v")).select("v").collect.head.getAs[Vector]("v")
// model.findSynonyms(vectors, 10).show()

import $ivy.`ch.qos.logback:logback-classic:1.2.3`
import $ivy.`com.typesafe.scala-logging::scala-logging:3.9.2`
import $ivy.`com.typesafe:config:1.4.0`
import $ivy.`com.github.fommil.netlib:all:1.1.2`
import $ivy.`org.apache.spark::spark-core:3.1.1`
import $ivy.`org.apache.spark::spark-mllib:3.1.1`
import $ivy.`org.apache.spark::spark-sql:3.1.1`
import $ivy.`com.github.pathikrit::better-files:3.8.0`
import $ivy.`com.typesafe.play::play-json:2.9.1`
import $ivy.`com.github.haifengl:smile-mkl:2.6.0`
import $ivy.`com.github.haifengl::smile-scala:2.6.0`
import $ivy.`com.johnsnowlabs.nlp:spark-nlp_2.12:3.0.3`
import org.apache.spark.broadcast._
import org.apache.spark.ml.feature._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel
import org.apache.spark.ml.fpm._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.ml.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.types._

object SparkSessionWrapper extends LazyLogging {
  logger.info("Spark Session init")
  lazy val sparkConf = new SparkConf()
    .set("spark.driver.maxResultSize", "0")
    .set("spark.debug.maxToStringFields", "2000")
    .set("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
    .setAppName("etl-generation")
    .setMaster("local[*]")

  lazy val spark: SparkSession =
    SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate
}

object ETL extends LazyLogging {

  def multicast(columnName: String, castType: DataType, newIdentityColumn: Column => Column): Column = {
    val c = col(columnName)
    when(c.isNotNull and c.startsWith("["),from_json(c, castType))
    .when(c.isNotNull and not(c.startsWith("[")), newIdentityColumn(c))
  }

  def apply(input: String, output: String): Unit = {
    import SparkSessionWrapper._
    import spark.implicits._
    implicit val ss: SparkSession = spark

    val namespaces = Seq("cell", "cl", "uberon")
    val columns = Seq(
      "id",
      "label",
      "isDeprecated",
      "oboNamespace",
      "consider",
      "synonyms"
    )

    logger.info("load json ontology classes")
    val owl = spark.read.json(input)
    val ont = owl.withColumn("id", element_at(split($"@id", ":"), 2))
      .withColumnRenamed("@type", "tp")
      .withColumn("isDeprecated",
        when(($"owl:deprecated".isNotNull and $"owl:deprecated" === false) or $"owl:deprecated".isNull, false)
          .otherwise(true))
      .filter($"hasOBONamespace".isInCollection(namespaces) and $"tp" === "owl:Class" and not($"label".startsWith("{")))
      .withColumn("consider", multicast("consider", ArrayType(StringType), array(_)))
//       .filter(not($"isDeprecated") or ($"isDeprecated" and $"consider".isNotNull))
      .withColumnRenamed("hasOBONamespace", "oboNamespace")
      .withColumn("label", multicast("hasExactSynonym", ArrayType(StringType), array(_)))
      .withColumn("hasExactSynonym", multicast("hasExactSynonym", ArrayType(StringType), array(_)))
      .withColumn("hasBroadSynonym", multicast("hasBroadSynonym", ArrayType(StringType), array(_)))
      .withColumn("hasRelatedSynonym", multicast("hasRelatedSynonym", ArrayType(StringType), array(_)))
      .withColumn("hasNarrowSynonym", multicast("hasNarrowSynonym", ArrayType(StringType), array(_)))
      .withColumn("synonyms", struct($"hasExactSynonym", $"hasBroadSynonym", $"hasRelatedSynonym", $"hasNarrowSynonym"))
      .select(columns.head, columns.tail:_*)

    ont.write.json(output)
  }
}

@main
def main(input: String,
         output: String): Unit =
  ETL(input, output)
