package io.opentargets.etl.backend.drug_beta

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.{ETLSessionContext, SparkHelpers}
import io.opentargets.etl.backend.SparkHelpers.IOResourceConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * This step will eventually replace the existing Drug step.
  *
  * It incorporates processing which was previously done in the `data-pipeline` project and consolidates all the logic in
  * this class.
  */
object DrugBeta extends LazyLogging {

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
      "drugbank" -> IOResourceConfig(common.inputs.drugDrugbank.format, common.inputs.drugDrugbank.path),
      // inputs from data-pipeline
      "efo" -> IOResourceConfig(common.inputs.disease.format, common.inputs.disease.path),
      "gene" -> IOResourceConfig(common.inputs.target.format, common.inputs.target.path)
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

    logger.info("Raw inputs for Drug beta loaded.")
    logger.info("Processing Drug beta transformations.")
    val molecule = new Molecule(moleculeDf, drugbankData)
    val indications = new Indication(indicationDf, efoDf)
    val mechanismOfAction = new MechanismOfAction(mechanismDf, targetDf, geneDf)

    logger.info("Joining molecules, indications, and mechanisms of action.")
    val drugDf = molecule.processMolecules
      .join(indications.processIndications, Seq("id"), "left_outer")
      .join(mechanismOfAction.processMechanismOfAction, Seq("id"), "left_outer")

    val outputs = Seq("drugs-beta")
    logger.info(s"Writing outputs: ${outputs.mkString(",")}")

    val outputConfs = SparkHelpers.generateDefaultIoOutputConfiguration(outputs: _*)(context.configuration)

    val outputDFs = (outputs zip Seq(drugDf)).toMap

    SparkHelpers.writeTo(outputConfs, outputDFs)
  }

}


