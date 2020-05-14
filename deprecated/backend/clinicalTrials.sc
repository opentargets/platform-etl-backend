import $file.common
import common._

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
      .withColumn(
        "drug_names",
        expr("transform(concat(array(pref_name), synonyms, trade_names), x -> lower(x))")
      )
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

  def loadClinicalTrials(
      inputs: Configuration.ClinicalTrials
  )(implicit ss: SparkSession): Map[String, DataFrame] = {
    def _loadCSV(path: String)(implicit ss: SparkSession) = {
      ss.read
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
      "interventionsMesh" -> _loadCSV(inputs.interventionsMesh),
      "conditions" -> _loadCSV(inputs.conditions),
      "conditionsMesh" -> _loadCSV(inputs.conditionsMesh)
    )
  }
}

object ClinicalTrials extends LazyLogging {
  def computeConditions(inputs: Map[String, DataFrame])(implicit ss: SparkSession): DataFrame = {
    import ss.implicits._

    val conditions = inputs("conditions")
    val conditionsMesh = inputs("conditionsMesh")

    val aggConditions = conditions
      .selectExpr("nct_id", "downcase_name as name")
      .groupBy($"nct_id")
      .agg(array_distinct(collect_list($"name")).as("condition_names"))

    val aggMeshes = conditionsMesh
      .selectExpr("nct_id", "downcase_mesh_term as mesh")
      .groupBy($"nct_id")
      .agg(array_distinct(collect_list($"mesh")).as("condition_mesh_terms"))

    val joint = aggConditions
      .join(aggMeshes, Seq("nct_id"), "full_outer")
      .withColumn(
        "condition_names",
        when($"condition_names".isNull, Array.empty[String])
          .otherwise($"condition_names")
      )
      .withColumn(
        "condition_mesh_terms",
        when($"condition_mesh_terms".isNull, Array.empty[String])
          .otherwise($"condition_mesh_terms")
      )

    joint
  }

  def computeInterventions(inputs: Map[String, DataFrame])(implicit ss: SparkSession): DataFrame = {
    import ss.implicits._

    val interventions = inputs("interventions")
    val otherNames = inputs("interventionsOtherNames")
    val meshes = inputs("interventionsMesh")

    val others = otherNames
    // .withColumn("name", rtrim(trim(lower($"name")), ","))
      .groupBy($"nct_id", $"intervention_id".as("id"))
      .agg(first($"name").as("name"), collect_set($"name").as("other_names"))
      .withColumn("other_names", concat($"other_names", array($"name")))
      .drop("name")

    val browse = meshes
      .groupBy($"nct_id")
      .agg(collect_set($"downcase_mesh_term").as("browse_names"))

    val inters = interventions.select("id", "nct_id", "intervention_type", "name")

    val jointInters = inters
      .join(others, Seq("id", "nct_id"), "full_outer")
      .join(browse, Seq("nct_id"), "full_outer")
      .withColumn(
        "browse_names",
        when($"browse_names".isNull, Array.empty[String])
          .otherwise($"browse_names")
      )
      .withColumn(
        "other_names",
        when($"other_names".isNull, Array.empty[String])
          .otherwise($"other_names")
      )
      .groupBy($"nct_id", $"intervention_type")
      .agg(
        collect_list($"name").as("names"),
        flatten(collect_list($"other_names")).as("other_names"),
        flatten(collect_list($"browse_names")).as("browse_names")
      )
      .groupBy($"nct_id")
      .agg(
        collect_list(
          struct(
            $"intervention_type",
            array_distinct($"names").as("names"),
            array_distinct($"other_names").as("other_names"),
            array_distinct($"browse_names").as("browse_names")
          )
        ).as("intervention_names")
      )

    jointInters
  }

  def apply(config: Config)(implicit ss: SparkSession) = {
    val commonSec = Configuration.loadCommon(config)
    val clinicalTrialsSec = Configuration.loadClinicalTrials(config)

    import ss.implicits._

//    val targets = Loaders.loadTargets(commonSec.inputs.target)
//    val diseases = Loaders.loadDiseases(commonSec.inputs.disease)
    val drugs = Loaders.loadDrugs(commonSec.inputs.drug.path)
    val ctMap = Loaders.loadClinicalTrials(clinicalTrialsSec)

    val studies = ctMap("studies")
      .withColumn(
        "has_expanded_access",
        when($"has_expanded_access" === "t", true)
          .otherwise(false)
      )
      .withColumn(
        "has_dmc",
        when($"has_dmc" === "t", true)
          .otherwise(false)
      )
      .withColumn(
        "is_fda_regulated_drug",
        when($"is_fda_regulated_drug" === "t", true)
          .otherwise(false)
      )
      .withColumn(
        "is_fda_regulated_device",
        when($"is_fda_regulated_device" === "t", true)
          .otherwise(false)
      )
      .withColumn(
        "phase",
        when(($"phase".isNull or ($"phase" === "") or ($"phase" === "n/a")), "not applicable")
          .when($"phase" === "phase 1/phase 2", "phase 2")
          .when($"phase" === "phase 2/phase 3", "phase 3")
          .when($"phase" === "early phase 1", "phase 0")
          .otherwise(lower($"phase"))
      )
      .withColumn("overall_status", lower(trim($"overall_status", "\"")))
      .persist()

    val references = ctMap("studyReferences")
      .groupBy($"nct_id")
      .agg(
        collect_set(when($"pmid".isNotNull, $"pmid")).as("pmids"),
        collect_list(when($"pmid".isNull, $"citation")).as("references")
      )

    val sponsors = ctMap("sponsors")
      .withColumn(
        "agency_class",
        when($"agency_class".isNull, "unknown").otherwise($"agency_class")
      )
      .groupBy($"nct_id", $"agency_class")
      .agg(collect_list($"name").as("names"))
      .groupBy($"nct_id")
      .agg(collect_list(struct($"agency_class", $"names")).as("agencies"))

    val countries = ctMap("countries")
      .withColumn("rem", when($"removed" === "t", true).otherwise(false))
      .where($"rem" === false)
      .groupBy($"nct_id")
      .agg(collect_set(lower($"name")).as("countries"))

    val interventions = computeInterventions(ctMap)
    val conditions = computeConditions(ctMap)

    // joining references
    val studiesWithCitations = studies
      .join(references, Seq("nct_id"), "left_outer")
      .withColumn("pmids", when($"pmids".isNull, Array.empty[Long]).otherwise($"pmids"))
      .withColumn(
        "references",
        when($"references".isNull, Array.empty[String]).otherwise($"references")
      )
      .join(countries, Seq("nct_id"), "left_outer")
      .join(sponsors, Seq("nct_id"), "left_outer")
      .join(interventions, Seq("nct_id"), "left_outer")
      .join(conditions, Seq("nct_id"), "left_outer")
      .persist()

    studies.unpersist()

    val drugsExploded = drugs
      .withColumn("drug_name_exploded", explode($"drug_names"))

    val studiesWithInterventions = studiesWithCitations
      .select("nct_id", "intervention_names")
      .withColumn(
        "intervention_names",
        expr(
          "flatten(transform(intervention_names, x -> concat(x.names,x.other_names,x.browse_names)))"
        )
      )
      .withColumn("intervention_name", explode($"intervention_names"))
      .join(drugsExploded, $"intervention_name" === $"drug_name_exploded", "inner")
      .selectExpr("nct_id", "drug_id", "drug_mechanisms_of_action")
      .distinct()
      .groupBy($"nct_id")
      .agg(collect_list(struct($"drug_id", $"drug_mechanisms_of_action")).as("drugs"))

    val ctWithDrug = studiesWithCitations
      .join(studiesWithInterventions, Seq("nct_id"), "left_outer")

//    logger.debug(s"number of clinical trials contained $numStudies studies")
//    ctWithDrug.sample(0.01D).write.json(commonSec.output + "/clinicaltrials_sample100/")
    ctWithDrug.write.json(commonSec.output + "/clinicaltrials/")

    // compute pseudo-evidences without condition efo resolution
    ctWithDrug
      .where(
        $"drugs".isNotNull and
          $"condition_mesh_terms".isNotNull and
          (size($"condition_mesh_terms") > 0) and
          (size($"drugs") > 0)
      )
      .withColumn("condition_mesh_term", explode($"condition_mesh_terms"))
      .withColumn("drug", explode($"drugs"))
      .withColumn(
        "targets",
        expr(
          "array_distinct(flatten(transform(drug.drug_mechanisms_of_action," +
            "x -> transform(x.target_components, " +
            "y -> y.ensembl))))"
        )
      )
      .where($"targets".isNotNull and (size($"targets") > 0))
      .withColumn("target", explode($"targets"))
      .write
      .json(commonSec.output + "/clinicaltrials_evidences/")
  }
}
