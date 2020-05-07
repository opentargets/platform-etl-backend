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
import scala.xml._

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
    import ss.implicits._

    def _loadXML(path: String)(implicit ss: SparkSession) = {
      import ss.implicits._

      val schema = StructType(
        Seq(
          StructField(name = "setId", dataType = StringType, nullable = false),
          StructField(name = "genericMedicine", dataType = StringType, nullable = true),
          StructField(name = "activeMoiety", dataType = StringType, nullable = true),
          StructField(name = "ingredientSubstances",
                      dataType = ArrayType(StringType),
                      nullable = true)
        )
      )

      val xmls = ss.sparkContext
        .wholeTextFiles(path)
        .map[Row](k => {
          val obj = XML.loadString(k._2)
          val setId = (obj \ "setId" \ "@root").text.toLowerCase()
          val ingredients = (obj \\ "ingredientSubstance")
          val genericMedicine = (obj \\ "genericMedicine" \ "name").headOption
            .map(_.text
              .trim()
              .replace(",","")
              .replace("  ", " ")
              .toLowerCase())
            .orNull
          val activeMoiety =
            (ingredients \\ "activeMoiety" \ "activeMoiety" \ "name").headOption.map(_.text
              .trim()
              .replace(",","")
              .replace("  ", " ")
              .toLowerCase())
            .orNull
          val ingredientNames = (ingredients \ "name").map(_.text
            .trim()
            .replace(",","")
            .replace("  ", " ")
            .toLowerCase()).toArray
          Row(setId, genericMedicine, activeMoiety, ingredientNames)
        })

      ss.createDataFrame(xmls, schema)
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
      "rxnorm" -> _loadCSV(inputs.rxnormMapping)
        .toDF("setId", "splVersion", "rxId", "rxString", "rxTTY")
        .withColumn("setId", lower($"setId"))
        .withColumn("rxString", lower($"rxString")),
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
      .orderBy($"setId")
      .persist()

    val prescription = ctMap("prescription")

//    prescription.write
//      .json(commonSec.output + "/dailymed/prescriptions/")

    rxnorm
      .join(prescription, Seq("setId"), "left_outer")
      .groupBy($"rxId")
      .agg(
        collect_set(lower($"rxString")).as("rxSynonyms"),
        collect_set(lower($"setId")).as("setIds"),
        collect_set($"genericMedicine").as("genericMedicine"),
        collect_set($"activeMoiety").as("activeMoiety"),
        array_distinct(flatten(collect_list($"ingredientSubstances"))).as("ingredientSubstances")
      )
      .write
      .json(commonSec.output + "/dailymed/rxnorms/")
  }
}
