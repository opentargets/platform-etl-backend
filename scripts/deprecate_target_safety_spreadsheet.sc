import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{
  array,
  array_union,
  coalesce,
  col,
  explode,
  lit,
  split,
  struct,
  transform,
  trim,
  typedLit
}

/**
  * The known target safety data comes from a spreadsheet (https://docs.google.com/spreadsheets/d/1EvpcnUkDASUNoBU5PzQPGD5YtZxh7cgotr2MqClJ7t0/edit#gid=650742396)
  * which is rarely (not since 2019) updated.
  *
  * This script is to convert that spreadsheet into flat json/parquet/csv files which can be used in the platform ETL.
  *
  * To run this script update the paths in the 'INPUTS' section to point to where to find the data. Inputs are as follows:
  *   - targetBetaDF: any output of the ETL 'target' step since the rewrite of target ~ June 2021
  *   - tsRawDF: adverse effects table from spreadsheet
  *   - efoCodesRawDF: efo mapping table from spreadsheet
  *   - uberonDF: Uberon mapping table from spreadsheet
  *   - srRawDF: safety risk table from spreadsheet
  *
  * Also specify where the outputs should be saved. Outputs are by default parquet files.
  *
  * The script is separated into UTILS, INPUTS, LOGIC, OUTPUTS
  *
  * UTILS is present because the logic makes use of helper functions which were written for the ETL. So this can be run
  * without dependencies they are pasted here so they are in scope.
  *
  * INPUTS points to input files
  *
  * LOGIC performs the manipulation of in input files into output structure.
  *
  * OUTPUTS saves the results of LOGIC.
  */
val ss: SparkSession = ???

import ss.implicits._

// UTILS - ignore for script
  def validateDF(requiredColumns: Set[String], dataFrame: DataFrame): Unit = {
    lazy val msg =
      s"One or more required columns (${requiredColumns.mkString(",")}) not found in dataFrame columns: ${dataFrame.columns
        .mkString(",")}"
    val columnsOnDf = dataFrame.columns.toSet
    assert(requiredColumns.forall(columnsOnDf.contains), msg)
  }
def safeArrayUnion(columns: Column*): Column = {
    columns.map(coalesce(_, typedLit(Array.empty))).reduce((c1, c2) => array_union(c1, c2))
  }
def createEnsemblToUniprotLookup(dataFrame: DataFrame): DataFrame = {
  import ss.implicits._
  validateDF(Set("id", "approvedSymbol", "proteinIds"), dataFrame)
  dataFrame
    .select(col("id"), array("approvedSymbol").as("as"), col("proteinIds.id").as("pid"))
    .select(col("id"), safeArrayUnion(col("as"), col("pid")).as("uniprot"))
    .select(col("id").as("ensemblId"), explode(col("uniprot")).as("uniprotId"))
}
val tsvWithHeader = (str: String) => ss.read.option("sep", "\\t").option("header", true).csv(str)

// INPUTS
val output = "/home/jarrod/development/platform-etl-backend/data/target-inputs/safety-revised-again/"
val data = "/home/jarrod/development/platform-etl-backend/data/target-inputs/safety/"

val targetBetaDF = createEnsemblToUniprotLookup(
  ss.read.json(
    "/home/jarrod/development/platform-etl-backend/data/dataproc-out/v8/target-beta/*.json"))

val tsRawDF =
  ss.read.option("sep", "\\t").option("header", true).csv(s"$data/adverse_effects.tsv")

// df: event, eventID
val efoCodesRawDF = ss.read
    .option("sep", "\\t")
    .option("header", true)
    .csv(s"$data/EFO_mapping.tsv")
    .select(col("Source term") as "event", $"Code" as "eventID")
// df: term, code
val uberonDF = tsvWithHeader(s"$data/UBERON_mapping.tsv").select(
    col("Publication term") as "term",
    col("UBERON code") as "code"
  )
// df: ref, target, term
val srRawDF = tsvWithHeader(s"$data/safety_risk_information.tsv")

// df ref, pmid, url
val referenceRawDF = ss.read
  .option("sep", "\\t")
  .option("header", true)
  .csv(s"$data/references.tsv")
  .select(col("Reference").as("ref"), col("PMID").as("pmid"), col("Other link").as("url"))


// LOGIC
/*
Returns a dataframe with all the sheets (except safety_risk) from target safety flattened
into a single structure.

Target safety data comes from a manually curated spreadsheet. This is not updated, and the data is
spread over multiple sheets. Most raw fields are ';' splittable strings. We want all the data
flat so we can filter restructure it easily. This method outputs a DF in the following form:

root
 |-- ensemblId: string (nullable = true)
 |-- uniprotId: string (nullable = true) -- raw data uses accession numbers to group
 |-- ref: string (nullable = true) -- ref, pmid, url are details of source
 |-- pmid: string (nullable = true)
 |-- url: string (nullable = true)
 |-- biologicalSystem: string (nullable = true) part of body affected: eg central nervous system
 |-- uberonCode: string (nullable = true) linked to biologicalSystem
 |-- symptom: string (nullable = true) - eg heart failure
 |-- efoId: string (nullable = true) efo code of symptom where available
 |-- effect: string (nullable = true) activation or inhibition
   */
  def translateTargetSafetyAdverseEffectsDF(targetSafetyDF: DataFrame,
                                            uberonDF: DataFrame,
                                            efoDF: DataFrame,
                                            ensgIdDF: DataFrame,
                                            tsReferenceDF: DataFrame): DataFrame = {
    val outputColumns = Seq("ensemblId",
      "target",
      "ref",
      "pmid",
      "url",
      "biologicalSystem",
      "uberonCode",
      "symptom",
      "efoId",
      "effect")
    def addEffect(dataFrame: DataFrame, effectName: String): DataFrame = {
      dataFrame
        .withColumn("symptom", explode(transform(split(col(effectName), ";"), s => trim(s))))
        .select(col("target"),
          col("ref"),
          struct(
            col("symptom"),
            lit(effectName) as "effect"
          ) as effectName)
    }
    def addEffects(dataFrame: DataFrame): DataFrame = {
      val cols = Array(
        "activation_acute",
        "activation_chronic",
        "activation_general",
        "activation_developmental toxicity",
        "inhibition_acute",
        "inhibition_chronic",
        "inhibition_developmental toxicity",
        "inhibition_general"
      )
      cols
        .foldLeft(dataFrame)((df, c) =>
          df.drop(c).join(addEffect(df, c), Seq("target", "ref"), "left_outer").distinct)
        .withColumn("effect", array(cols.head, cols.tail: _*))
        .drop(cols: _*)
        .withColumn("e", explode(col("effect")))
        .select(col("target"), col("biologicalSystem"), col("ref"), col("e.*"))
        .filter(col("symptom").isNotNull && col("effect").isNotNull)
        .distinct
    }
    def addUberon(dataFrame: DataFrame): DataFrame =
      dataFrame
        .join(uberonDF, col("biologicalSystem") === col("term"), "left_outer")
        .drop(col("term"))
        .withColumnRenamed("code", "uberonCode")
    def addEfo(dataFrame: DataFrame): DataFrame =
      dataFrame
        .join(efoDF, col("symptom") === col("event"), "left_outer")
        .drop("event")
        .withColumnRenamed("eventID", "efoId")
    def addEnsemblId(dataFrame: DataFrame): DataFrame =
      dataFrame
        .join(ensgIdDF, col("target") === col("uniprotId"), "left_outer")
        .drop("uniprotId")
    def addReferences(dataFrame: DataFrame): DataFrame =
      dataFrame
        .join(tsReferenceDF, Seq("ref"), "left_outer")

    val cols = Array(
      ("Ref", "ref"),
      ("Target", "target"),
      ("Main organ/system affected", "biologicalSystem"),
      ("Agonism/Activation effects_Acute dosing", "activation_acute"),
      ("Agonism/Activation effects_Chronic dosing", "activation_chronic"),
      ("Agonism/Activation effects_Developmental toxicity", "activation_developmental toxicity"),
      ("Agonism/Activation effects_General", "activation_general"),
      ("Antagonism/Inhibition effects_Acute dosing", "inhibition_acute"),
      ("Antagonism/Inhibition effects_Chronic dosing", "inhibition_chronic"),
      ("Antagonism/Inhibition effects_Developmental toxicity", "inhibition_developmental toxicity"),
      ("Antagonism/Inhibition effects_General", "inhibition_general")
    )
    val newNames = cols.map(_._2)

    val baseDF = cols
      .foldLeft(targetSafetyDF)((df, names) => df.withColumnRenamed(names._1, names._2))
      .select(newNames.head, newNames.tail: _*)
      .withColumn("biologicalSystem", explode(transform(split(col("biologicalSystem"), ";"), s => trim(s))))
      .withColumn("ref", explode(transform(split(col("ref"), ";"), s => trim(s))))

    addEffects(baseDF)
      .transform(addUberon)
      .transform(addEfo)
      .transform(addEnsemblId)
      .transform(addReferences)
      .select(outputColumns.map(col): _*)
      .distinct
  }

val aeDF = translateTargetSafetyAdverseEffectsDF(tsRawDF,
                                                   uberonDF,
                                                   efoCodesRawDF,
                                                   targetBetaDF,
                                                   referenceRawDF)

/*
Returns a dataframe with the flatten contents of 'safety risk' sheet from target safety data.

Outputs dataframe with:
root
 |-- ensemblId: string (nullable = true)
 |-- uniprotId: string (nullable = true)
 |-- term: string (nullable = true)
 |-- uberonId: string (nullable = true)
 |-- ref: string (nullable = true)
 |-- pmid: string (nullable = true)
 |-- url: string (nullable = true)
   */
def translateTargetSafetySafetyRiskDF(dataFrame: DataFrame,
                                        uberon: DataFrame,
                                        references: DataFrame,
                                        ensgIds: DataFrame): DataFrame = {
    val df = dataFrame
      .select(
        explode(split(col("Reference"), ";")) as "ref",
        col("Target") as "target",
        col("Main organ/system affected") as "term",
        col("Safety liability") as "liability",
      )
      .select(
        col("target"),
        col("term"),
        col("liability"),
        trim(col("ref")) as "ref"
      )
      .join(uberon, Seq("term"), "left_outer")
      .withColumnRenamed("code", "uberonId")
      .withColumnRenamed("term", "biologicalSystem")
      .join(references, Seq("ref"), "left_outer")
      .join(ensgIds, col("target") === col("uniprotId"))
      .drop("uniprotId")

  df.select("ensemblId", "target", "biologicalSystem", "uberonId", "liability", "ref", "pmid", "url")
}
val srDF = translateTargetSafetySafetyRiskDF(srRawDF, uberonDF, referenceRawDF, targetBetaDF)

// OUTPUTS
aeDF.distinct.write.parquet(output + "ae_safety")
srDF.distinct.write.parquet(output + "sr_safety")



