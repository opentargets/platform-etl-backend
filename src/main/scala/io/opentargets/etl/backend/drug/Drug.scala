package io.opentargets.etl.backend.drug

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.drug.DrugCommon._
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/** This step will eventually replace the existing Drug step.
  *
  * It incorporates processing which was previously done in the `data-pipeline` project and consolidates all the logic in
  * this class.
  */
object Drug extends Serializable with LazyLogging {

  def apply()(implicit context: ETLSessionContext): Unit = {
    implicit val ss: SparkSession = context.sparkSession

    val drugConfiguration = context.configuration.drug
    val outputs = drugConfiguration.outputs

    logger.info("Loading raw inputs for Drug beta step.")
    val mappedInputs = Map(
      "indication" -> drugConfiguration.chemblIndication,
      "mechanism" -> drugConfiguration.chemblMechanism,
      "molecule" -> drugConfiguration.chemblMolecule,
      "target" -> drugConfiguration.chemblTarget,
      "warnings" -> drugConfiguration.chemblWarning,
      "drugbankChemblMap" -> drugConfiguration.drugbankToChembl,
      "efo" -> drugConfiguration.diseaseEtl,
      "gene" -> drugConfiguration.targetEtl,
      "evidence" -> drugConfiguration.evidenceEtl
    )

    val inputDataFrames = IoHelpers.readFrom(mappedInputs)

    // raw input dataframes
    lazy val moleculeDf: DataFrame = inputDataFrames("molecule").data
    lazy val mechanismDf: DataFrame = inputDataFrames("mechanism").data
    lazy val indicationDf: DataFrame = inputDataFrames("indication").data
    lazy val targetDf: DataFrame = inputDataFrames("target").data
    lazy val geneDf: DataFrame = inputDataFrames("gene").data
    lazy val drugbank2ChemblMap: DataFrame = inputDataFrames("drugbankChemblMap").data
      .withColumnRenamed("From src:'1'", "id")
      .withColumnRenamed("To src:'2'", "drugbank_id")
    lazy val efoDf: DataFrame = inputDataFrames("efo").data
    lazy val evidenceDf: DataFrame = inputDataFrames("evidence").data
    lazy val warningRawDf: DataFrame = inputDataFrames("warnings").data

    // processed dataframes
    logger.info("Raw inputs for Drug loaded.")
    logger.info("Processing Drug transformations.")
    val indicationProcessedDf = Indication(indicationDf, efoDf)
    val moleculeProcessedDf =
      Molecule(moleculeDf, drugbank2ChemblMap, drugConfiguration.drugExtensions)
    val mechanismOfActionProcessedDf: DataFrame =
      MechanismOfAction(mechanismDf, targetDf, geneDf)
    val targetsAndDiseasesDf =
      DrugCommon.getUniqTargetsAndDiseasesPerDrugId(evidenceDf).withColumnRenamed("drugId", "id")
    val warningsDF = DrugWarning(warningRawDf)

    logger.whenTraceEnabled {
      val columnString: DataFrame => String = _.columns.mkString("Columns: [", ",", "]")
      logger.trace(s"""Intermediate dataframes:
             Columns:
             \n\t Molecule: ${columnString(moleculeProcessedDf)},
             \n\t Indications: ${columnString(indicationDf)},
             \n\t Mechanisms: ${columnString(mechanismOfActionProcessedDf)},
             \n\t Linkages: ${columnString(targetsAndDiseasesDf)}
             Row counts:
             \n\t Molecule: ${moleculeProcessedDf.count},
             \n\t Indications: ${indicationDf.count},
             \n\t Mechanisms: ${mechanismOfActionProcessedDf.count},
             \n\t Linkages: ${targetsAndDiseasesDf.count}
             """)
    }

    logger.info(
      "Joining molecules, indications, mechanisms of action, and target and disease linkages."
    )

    // We define a drug as having either a drugbank id, a mechanism of action, or an indication.
    val isDrugMolecule: Column = array_contains(map_keys(col("crossReferences")), "drugbank") ||
      col("indications").isNotNull ||
      col("mechanismsOfAction").isNotNull

    // using left_outer joins as we want to keep all molecules until the filter clause which defines a 'drug' for the
    // purposes of the index.
    val drugDf: DataFrame = moleculeProcessedDf
      .join(
        indicationProcessedDf
          .select("id", "indications"),
        Seq("id"),
        "left_outer"
      )
      .join(
        mechanismOfActionProcessedDf
          .select(explode(col("chemblIds")).as("id"))
          .distinct
          .withColumn("mechanismsOfAction", lit(true)),
        Seq("id"),
        "left_outer"
      )
      .join(targetsAndDiseasesDf, Seq("id"), "left_outer")
      .filter(isDrugMolecule)
      .transform(addDescription)
      .drop("indications", "mechanismsOfAction", "withdrawnNotice ")
      .transform(cleanup)

    val dataframesToSave: IOResources = Map(
      "drug" -> IOResource(drugDf, outputs.drug),
      "mechanism_of_action" -> IOResource(mechanismOfActionProcessedDf, outputs.mechanismOfAction),
      "indication" -> IOResource(indicationProcessedDf, outputs.indications),
      "drug_warnings" -> IOResource(warningsDF, outputs.warnings)
    )

    IoHelpers.writeTo(dataframesToSave)
  }

  /*
  Final tidying up that aren't business logic but are nice to have for consistent outputs.
   */
  def cleanup(df: DataFrame): DataFrame = {
    // add empty collection as value instead of null values.
    Seq("tradeNames", "synonyms").foldLeft(df)((dataF, column) => {
      dataF.withColumn(column, coalesce(col(column), typedLit(Seq.empty)))
    })
  }

}
