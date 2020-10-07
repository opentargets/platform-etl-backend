package io.opentargets.etl.backend.drug_beta

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.{ETLSessionContext, SparkHelpers}
import io.opentargets.etl.backend.SparkHelpers.IOResourceConfig
import org.apache.spark.sql.functions.{col, size}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * This step will eventually replace the existing Drug step.
  *
  * It incorporates processing which was previously done in the `data-pipeline` project and consolidates all the logic in
  * this class.
  */
object DrugBeta extends Serializable with LazyLogging {

  def apply()(implicit context: ETLSessionContext): Unit = {
    implicit val ss: SparkSession = context.sparkSession

    import ss.implicits._

    val common = context.configuration.common

    logger.info("Loading raw inputs for Drug beta step.")
    val mappedInputs = Map(
      "indication" -> IOResourceConfig(common.inputs.drugChemblIndication.format,
                                       common.inputs.drugChemblIndication.path),
      "mechanism" -> IOResourceConfig(common.inputs.drugChemblMechanism.format,
                                      common.inputs.drugChemblMechanism.path),
      "molecule" -> IOResourceConfig(common.inputs.drugChemblMolecule.format,
                                     common.inputs.drugChemblMolecule.path),
      "target" -> IOResourceConfig(common.inputs.drugChemblTarget.format,
                                   common.inputs.drugChemblTarget.path),
      "drugbank" -> IOResourceConfig(common.inputs.drugDrugbank.format,
                                     common.inputs.drugDrugbank.path,
                                     Some("\\t"),
                                     Some(true)),
      // inputs from data-pipeline
      "efo" -> IOResourceConfig(common.inputs.disease.format, common.inputs.disease.path),
      "gene" -> IOResourceConfig(common.inputs.target.format, common.inputs.target.path),
      "evidence" -> IOResourceConfig(common.inputs.evidence.format, common.inputs.evidence.path)
    )

    val inputDataFrames = SparkHelpers.readFrom(mappedInputs)

    lazy val moleculeDf: DataFrame = inputDataFrames("molecule")
    lazy val mechanismDf: DataFrame = inputDataFrames("mechanism")
    lazy val indicationDf: DataFrame = inputDataFrames("indication")
    lazy val targetDf: DataFrame = inputDataFrames("target")
    lazy val geneDf: DataFrame = inputDataFrames("gene")
    lazy val drugbankData: DataFrame = inputDataFrames("drugbank")
      .withColumnRenamed("From src:'1'", "id")
      .withColumnRenamed("To src:'2'", "drugbank_id")
    lazy val efoDf: DataFrame = inputDataFrames("efo")
    lazy val evidenceDf: DataFrame = inputDataFrames("evidence")

    logger.info("Raw inputs for Drug beta loaded.")
    logger.info("Processing Drug beta transformations.")
    val molecule = new Molecule(moleculeDf, drugbankData)
    val indications = new Indication(indicationDf, efoDf)
    val mechanismOfAction = new MechanismOfAction(mechanismDf, targetDf, geneDf)

    val moleculeProcessedDf = molecule.processMolecules
    val indicationProcessedDf = indications.processIndications
    val mechanismOfActionProcessedDf = mechanismOfAction.processMechanismOfAction
    val targetsAndDiseasesDf = DrugCommon.getUniqTargetsAndDiseasesPerDrugId(evidenceDf).withColumnRenamed("drug_id", "id")
    printDetailedLogging(moleculeProcessedDf, indicationProcessedDf, mechanismOfActionProcessedDf, targetsAndDiseasesDf)

    logger.info("Joining molecules, indications, mechanisms of action, and target and disease linkages.")
    // using inner joins as we don't want molecules that have no indications and mechanisms of action.
    val drugDf: DataFrame = moleculeProcessedDf
      .join(mechanismOfActionProcessedDf, Seq("id"))
      .join(indicationProcessedDf, Seq("id"))
      .join(targetsAndDiseasesDf, Seq("id"), "left_outer")
      .transform(addDescription)

    val outputs = Seq("drugs-beta")
    logger.info(s"Writing outputs: ${outputs.mkString(",")}")

    val outputConfs =
      SparkHelpers.generateDefaultIoOutputConfiguration(outputs: _*)(context.configuration)

    val outputDFs = (outputs zip Seq(drugDf)).toMap

    SparkHelpers.writeTo(outputConfs, outputDFs)
  }

  // Effectively a wrapper around the 'description` UDF: isolating in function so the adding/dumping necessary
  // columns doesn't clutter logic in the apply method. Note: this should be applied after all other transformations!
  def addDescription(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumn("_indication_phases", col("indications.rows.maxPhaseForIndication"))
      .withColumn("_indication_labels", col("indications.rows.disease"))
      .transform(DrugCommon.addDescriptionField)
      .drop("_indication_phases", "_indication_labels")
  }

  private def printDetailedLogging(molecule: DataFrame, indication:DataFrame, mechanismOfAction: DataFrame, targetsAndDiseases: DataFrame): Unit = {
    val columnString: DataFrame => String = _.columns.mkString("Columns: [", ",", "]")
    logger.trace(s"""Intermediate dataframes:
    Columns:
    \n\t Molecule: ${columnString(molecule)},
    \n\t Indications: ${columnString(indication)},
    \n\t Mechanisms: ${columnString(mechanismOfAction)},
    \n\t Linkages: ${columnString(targetsAndDiseases)}
    Row counts:
    \n\t Molecule: ${molecule.count},
    \n\t Indications: ${indication.count},
    \n\t Mechanisms: ${mechanismOfAction.count},
    \n\t Linkages: ${targetsAndDiseases.count}
    """)

  }

}
