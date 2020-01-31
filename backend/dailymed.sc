import $file.common
import common._

import $ivy.`ch.qos.logback:logback-classic:1.2.3`
import $ivy.`com.typesafe.scala-logging::scala-logging:3.9.2`
import $ivy.`com.typesafe:config:1.4.0`
import $ivy.`com.github.fommil.netlib:all:1.1.2`
import $ivy.`org.apache.spark::spark-core:2.4.3`
import $ivy.`org.apache.spark::spark-mllib:2.4.3`
import $ivy.`org.apache.spark::spark-sql:2.4.3`
import $ivy.`com.github.pathikrit::better-files:3.8.0`
import $ivy.`com.typesafe.play::play-json:2.7.3`
import $ivy.`com.databricks::spark-xml:0.8.0`

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import better.files.Dsl._
import better.files._
import com.typesafe.config.{Config, ConfigFactory, ConfigObject, ConfigRenderOptions}
import com.typesafe.scalalogging.{LazyLogging, Logger}
import com.databricks.spark.xml._

import scala.math.pow

object Loaders extends LazyLogging {
  def loadTargets(path: String)(implicit ss: SparkSession): DataFrame = {
    logger.info("load targets jsonl")
    val targets = ss.read.json(path)
    targets
  }

  def loadDiseases(path: String)(implicit ss: SparkSession): DataFrame = {
    logger.info("load diseases jsonl")
    val diseaseList = ss.read.json(path)

    // generate needed fields as ancestors
    val efos = diseaseList
      .withColumn("disease_id", substring_index(col("code"), "/", -1))
      .withColumn("ancestors", flatten(col("path_codes")))

    // compute descendants
    val descendants = efos
      .where(size(col("ancestors")) > 0)
      .withColumn("ancestor", explode(col("ancestors")))
      // all diseases have an ancestor, at least itself
      .groupBy("ancestor")
      .agg(collect_set(col("disease_id")).as("descendants"))
      .withColumnRenamed("ancestor", "disease_id")

    val diseases = efos.join(descendants, Seq("disease_id"))
    diseases
  }

  def loadDrugs(path: String)(implicit ss: SparkSession): DataFrame = {
    import ss.implicits._

    logger.info("load drugs jsonl")
    val drugs = ss.read
      .json(path)
      .where($"number_of_mechanisms_of_action" > 0)
      .withColumn("drug_names",
                  expr("transform(concat(array(pref_name), synonyms, trade_names), x -> lower(x))"))
      .selectExpr(
        "id as drug_id",
        "lower(pref_name) as drug_name",
        "drug_names",
        "indications as drug_indications",
        "mechanisms_of_action as drug_mechanisms_of_action",
        "indication_therapeutic_areas as drug_indication_therapeutic_areas"
      )

    drugs
  }

  def loadDailymed(inputs: Configuration.Dailymed)(
      implicit ss: SparkSession): Map[String, DataFrame] = {
    def _loadXML(path: String)(implicit ss: SparkSession) = {
      ss.read
        .option("rowTag", "document")
        .xml(path)
    }

    def _loadCSV(path: String)(implicit ss: SparkSession) = {
      ss.read
        .option("sep", "|")
        .option("mode", "DROPMALFORMED")
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
    }

    Map(
      "rxnorm" -> _loadCSV(inputs.rxnormMapping),
      "prescription" -> _loadXML(inputs.prescriptionData)
    )
  }
}

object Dailymed extends LazyLogging {
  def apply(config: Config)(implicit ss: SparkSession) = {
    val commonSec = Configuration.loadCommon(config)
    val dailymedSec = Configuration.loadDailymed(config)

    import ss.implicits._

//    val targets = Loaders.loadTargets(commonSec.inputs.target)
//    val diseases = Loaders.loadDiseases(commonSec.inputs.disease)
//    val drugs = Loaders.loadDrugs(commonSec.inputs.drug)

    val ctMap = Loaders.loadDailymed(dailymedSec)

    val rxnorm = ctMap("rxnorm")
    val prescription = ctMap("prescription")

    prescription
      .write
      .json(commonSec.output + "/dailymed/prescriptions/")

    rxnorm
      .groupBy($"RXCUI".as("rx_id"))
      .agg(collect_set(lower($"RXSTRING")).as("rx_synonyms"),
        collect_set(lower($"SETID")).as("set_ids"))
      .write
      .json(commonSec.output + "/dailymed/rxnorms/")
  }
}
