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

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import better.files.Dsl._
import better.files._
import com.typesafe.config.{Config, ConfigFactory, ConfigObject, ConfigRenderOptions}
import com.typesafe.scalalogging.{LazyLogging, Logger}

import scala.math.pow

object Loaders extends LazyLogging {
  def loadTargets(path: String)(implicit ss: SparkSession): DataFrame = {
    logger.info("load targets jsonl")
    val targets = ss.read.json(path)
    targets
  }

  def loadExpressions(path: String)(implicit ss: SparkSession): DataFrame = {
    logger.info("load expressions jsonl")
    val expressions = ss.read.json(path)
    expressions
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
    logger.info("load drugs jsonl")
    val drugs = ss.read.json(path)
    drugs
  }

  def loadClinicalTrials(inputs: Configuration.ClinicalTrials)(implicit ss: SparkSession): Map[String, DataFrame] = {
    def _loadCSV(path: String)(implicit ss: SparkSession) = {
      ss
        .read
        .option("sep", "|")
        .option("mode", "DROPMALFORMED")
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
    }

    Map(
      "studies" -> _loadCSV(inputs.studies),
      "studyReferences" -> _loadCSV(inputs.studyReferences),
      "countries" -> _loadCSV(inputs.countries),
      "sponsors" -> _loadCSV(inputs.sponsors),
      "interventions" -> _loadCSV(inputs.interventions),
      "interventionsOtherNames" -> _loadCSV(inputs.interventionsOtherNames),
      "interventionsMesh" -> _loadCSV(inputs.interventionsMesh)
    )
  }
}

object ClinicalTrials extends LazyLogging {
  def apply(config: Config)(implicit ss: SparkSession) = {
    val associationsSec = Configuration.loadAssociationSection(config)
    val commonSec = Configuration.loadCommon(config)

    import ss.implicits._

//    val targets = Loaders.loadTargets(commonSec.inputs.target)
//    val diseases = Loaders.loadDiseases(commonSec.inputs.disease)
//    val drugs = Loaders.loadDrugs(commonSec.inputs.drug)

    val ctMap = Loaders.loadClinicalTrials(commonSec.inputs.clinicalTrials)

    val studies = ctMap("studies")
      .withColumn("has_expanded_access", when($"has_expanded_access" === "t", true)
        .otherwise(false))
      .withColumn("has_dmc", when($"has_dmc" === "t", true)
        .otherwise(false))
      .withColumn("is_fda_regulated_drug", when($"is_fda_regulated_drug" === "t", true)
        .otherwise(false))
      .withColumn("is_fda_regulated_device", when($"is_fda_regulated_device" === "t", true)
        .otherwise(false))
      .withColumn("phase", when($"phase".isNull, "N/A").otherwise($"phase"))
      .persist()

    val references = ctMap("studyReferences")
      .groupBy($"nct_id")
      .agg(collect_set(when($"pmid".isNotNull, $"pmid")).as("pmids"),
        collect_list(when($"pmid".isNull, $"citation")).as("references"))

    val sponsors = ctMap("sponsors")
      .groupBy($"nct_id")
      .pivot($"agency_class")
      .agg(collect_list($"name"))


    val countries = ctMap("countries")
      .withColumn("rem",when($"removed" === "t", true).otherwise(false))
      .where($"rem" === false)
      .groupBy($"nct_id")
      .agg(collect_set(lower($"name")).as("countries"))

    // studies aggregations
    val numStudies = studies.count()
    val grouppedPhases = studies.groupBy($"phase", $"overall_status")
      .count()

    // joining references
    val studiesWithCitations = studies.join(references, Seq("nct_id"), "left_outer")
      .withColumn("pmids", when($"pmids".isNull, Array.empty[Long]).otherwise($"pmids"))
      .withColumn("references", when($"references".isNull, Array.empty[String]).otherwise($"references"))
      .join(countries, Seq("nct_id"), "left_outer")
      .join(sponsors, Seq("nct_id"), "left_outer")

    logger.debug(s"number of clinical trials contained $numStudies studies")
    studiesWithCitations.sample(0.01D).write.json(commonSec.output + "/clinicaltrials_sample100/")
//    studiesWithCitations.write.json(commonSec.output + "/clinicaltrials/")
    grouppedPhases.write.json(commonSec.output + "/clinicaltrials_phase_status/")

    /*
      interventions - select(id(intervention_id)|nct_id|intervention_type|name)
        1. concat(array_distinct(trim(lower(split(name)))),lower(name)) as names
      invervention_other_names - group by (nct_id,intervention_id)
        1. concat(array_distinct(rtrim(',',trim(lower(split(collect_set(name))))), array(lower(name))) as other_names
      browse_interventions - group by (nct_id) collect_set(lower(downcase_mesh_term) as browse_names
        4. interventions
          left join intervention_other_names by intervention_id as id, nct_id
          left join bwose_interventions by nct_id
        5. inteventions - create intervention_all_names = concat(names, other_names, browse_names)
     */

    /*

     */
  }
}

