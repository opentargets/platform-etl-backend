package io.opentargets.etl.backend.targetEngine

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.spark.IOResource
import io.opentargets.etl.backend.spark.IoHelpers.{IOResources, readFrom, writeTo}
import io.opentargets.etl.backend.targetEngine.Functions._
import io.opentargets.etl.backend.targetEngine.UniprotLocationFunctions.FindParentChidCousins
import org.apache.spark.sql.functions.{col, struct, to_json}
import org.apache.spark.sql.types.StringType
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
      "uniprotSlterms" -> config.uniprotSlterms,
      "mousePhenoScores" -> config.mousePhenoScores
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
    val mousePhenoScoresDF = inputs("mousePhenoScores").data

    val parentChildCousinsDF = FindParentChidCousins(uniprotDF)

    val querysetDF = targetsDF
      .select(col("id").as("targetid"))

    val fullTable = querysetDF
      .transform(biotypeQuery(_, targetsDF))
      .transform(targetMembraneQuery(_, targetsDF, parentChildCousinsDF))
      .transform(ligandPocketQuery(_, targetsDF))
      .transform(safetyQuery(_, targetsDF))
      .transform(constraintQuery(_, targetsDF))
      .transform(paralogsQuery(_, targetsDF))
      .transform(orthologsMouseQuery(_, targetsDF))
      .transform(driverGeneQuery(_, targetsDF))
      .transform(tepQuery(_, targetsDF))
      .transform(mousemodQuery(_, mouseDF, mousePhenoScoresDF))
      .transform(chemicalProbesQuery(_, targetsDF))
      .transform(clinTrialsQuery(_, moleculeDF, moleculeMecDF))
      .transform(tissueSpecificQuery(_, hpaDataDF))

    fullTable.select(
      col("targetid").as("targetId"),
      col("Nr_mb").as("isInMembrane"),
      col("Nr_secreted").as("isSecreted"),
      col("Nr_Event").as("hasSafetyEvent"),
      col("Nr_Pocket").as("hasPocket"),
      col("Nr_Ligand").as("hasLigand"),
      col("Nr_sMBinder").as("hasSmallMoleculeBinder"),
      col("cal_score").as("geneticConstraint"),
      col("Nr_paralogs").as("paralogMaxIdentityPercentage"),
      col("Nr_ortholog").as("mouseOrthologMaxIdentityPercentage"),
      col("Nr_CDG").as("isCancerDriverGene"),
      col("Nr_TEP").as("hasTEP"),
      col("scaledHarmonicSum").as("MouseKOScore"),
      col("Nr_chprob").as("hasHighQualityChemicalProbes"),
      col("inClinicalTrials").as("maxClinicalTrialPhase"),
      col("Nr_specificity").as("tissueSpecificity"),
      col("Nr_distribution").as("tissueDistribution")
    )
  }

  def writeOutput(targetEngineDF: DataFrame)(implicit context: ETLSessionContext): Unit = {
    val outputConfig = context.configuration.targetEngine.outputs.targetEngine

    val dataFramesToSave = Map("targetEngine" -> IOResource(targetEngineDF, outputConfig))

    writeTo(dataFramesToSave)
  }

}
