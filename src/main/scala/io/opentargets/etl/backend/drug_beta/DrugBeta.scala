package io.opentargets.etl.backend.drug_beta

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.{ETLSessionContext, SparkHelpers}
import io.opentargets.etl.backend.SparkHelpers.IOResourceConfig
import io.opentargets.etl.backend.drug_beta.Molecule
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * This step will eventually replace the existing Drug step.
  *
  * It incorporates processing which was previously done in the `data-pipeline` project and consolidates all the logic in
  * this class.
  */
object DrugBeta extends LazyLogging {

  def apply()(implicit context: ETLSessionContext) = {
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
      "drugbank" -> IOResourceConfig(common.inputs.drugDrugbank.format, common.inputs.drugDrugbank.path)
    )

    val inputDataFrames = SparkHelpers.readFrom(mappedInputs)

    val moleculeDf: DataFrame = inputDataFrames("molecule")
    val mechanismDf: DataFrame = inputDataFrames("mechanism")
    val indicationDf: DataFrame = inputDataFrames("indication")
    val targetDf: DataFrame = inputDataFrames("target")
    val drugbankData: DataFrame = inputDataFrames("drugbank")
      .withColumnRenamed("From src:'1'", "id")
      .withColumnRenamed("To src:'2'", "drugbank_id")

    logger.info("Raw inputs for Drug beta loaded.")

    val molecule = new Molecule(moleculeDf, drugbankData)

    def mechanismPreprocess(df: DataFrame): DataFrame = ???

    def indicationPreprocess(df: DataFrame): DataFrame = ???

    def targetPreprocess(df: DataFrame): DataFrame = ???

  }

}


