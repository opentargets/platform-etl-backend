package io.opentargets.etl.backend.targetEngine

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.spark.IOResource
import io.opentargets.etl.backend.spark.IoHelpers.{IOResources, readFrom, writeTo}
import io.opentargets.etl.backend.targetEngine.Functions._
import io.opentargets.etl.backend.targetEngine.UniprotLocationFunctions.FindParentChidCousins
import org.apache.spark.sql.{DataFrame, SparkSession}

object TargetEngine extends LazyLogging {

  def apply()(implicit context: ETLSessionContext): Unit = {
    implicit val ss: SparkSession = context.sparkSession

    val inputs = readInputs

    val targetEngineDF = compute(inputs)

    writeOutput(targetEngineDF)

  }

  def readInputs()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession

    val config = context.configuration.targetEngine.inputs

    val mappedInputs = Map(
      "targets" -> config.targets,
      "molecule" -> config.molecule,
      "mouse" -> config.mousePhenotypes,
      "moleculeMec" -> config.mechanismOfAction,
      "hpaData" -> config.hpaData,
      "uniprotSlterms" -> config.uniprotSlterms
    )

    readFrom(mappedInputs)

  }

  def compute(inputs: IOResources)(implicit ss: SparkSession): DataFrame = {
    val targetsDF = inputs("targets").data
    val mouseDF = inputs("mouse").data
    val moleculeDF = inputs("molecule").data
    val moleculeMecDF = inputs("moleculeMec").data
    val hpaDataDF = inputs("hpaData").data
    val uniprotDF = inputs("uniprotSlterms").data

    val parentChildCousinsDF = FindParentChidCousins(uniprotDF)

    targetsDF
      .transform(biotypeQuery)
      .transform(targetMembraneQuery(_, targetsDF, parentChildCousinsDF))
      .transform(ligandPocketQuery(_, targetsDF))
      .transform(safetyQuery(_, targetsDF))
      .transform(constraintQuery(_, targetsDF))
      .transform(paralogsQuery(_, targetsDF))
      .transform(orthologsMouseQuery(_, targetsDF))
      .transform(driverGeneQuery(_, targetsDF))
      .transform(tepQuery(_, targetsDF))
      .transform(mousemodQuery(_, mouseDF))
      .transform(chemicalProbesQuery(_, targetsDF))
      .transform(clinTrialsQuery(_, moleculeDF, moleculeMecDF))
      .transform(tissueSpecificQuery(_, hpaDataDF))
  }

  def writeOutput(targetEngineDF: DataFrame)(implicit context: ETLSessionContext): Unit = {
    val outputConfig = context.configuration.targetEngine.outputs.targetEngine

    val dataFramesToSave = Map("targetEngine" -> IOResource(targetEngineDF, outputConfig))

    writeTo(dataFramesToSave)
  }

}
