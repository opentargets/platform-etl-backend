package io.opentargets.etl.backend.drug

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.drug.DrugCommon._
import io.opentargets.etl.backend.spark.Helpers
import io.opentargets.etl.backend.spark.Helpers.{IOResourceConfig, IOResourceConfs, IOResources, nest}
import org.apache.spark.sql.functions.{array_contains, coalesce, col, map_keys, typedLit}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * This step will eventually replace the existing Drug step.
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
      "drugbankChemblMap" -> drugConfiguration.drugbankToChembl,
      "efo" -> drugConfiguration.diseaseEtl,
      "gene" -> drugConfiguration.targetEtl,
      "evidence" -> drugConfiguration.evidenceEtl
    )

    val inputDataFrames = Helpers.readFrom(mappedInputs)

    // raw input dataframes
    lazy val moleculeDf: DataFrame = inputDataFrames("molecule")
    lazy val mechanismDf: DataFrame = inputDataFrames("mechanism")
    lazy val indicationDf: DataFrame = inputDataFrames("indication")
    lazy val targetDf: DataFrame = inputDataFrames("target")
    lazy val geneDf: DataFrame = inputDataFrames("gene")
    lazy val drugbank2ChemblMap: DataFrame = inputDataFrames("drugbankChemblMap")
      .withColumnRenamed("From src:'1'", "id")
      .withColumnRenamed("To src:'2'", "drugbank_id")
    lazy val efoDf: DataFrame = inputDataFrames("efo")
    lazy val evidenceDf: DataFrame = inputDataFrames("evidence")

    // processed dataframes
    logger.info("Raw inputs for Drug beta loaded.")
    logger.info("Processing Drug beta transformations.")
    val mechanismOfActionProcessedDf: DataFrame = MechanismOfAction(mechanismDf, targetDf, geneDf)
    val indicationProcessedDf = Indication(indicationDf, efoDf)
    val moleculeProcessedDf =
      Molecule(moleculeDf, drugbank2ChemblMap, drugConfiguration.drugExtensions)
    val targetsAndDiseasesDf =
      DrugCommon.getUniqTargetsAndDiseasesPerDrugId(evidenceDf).withColumnRenamed("drugId", "id")

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
      "Joining molecules, indications, mechanisms of action, and target and disease linkages.")

    // We define a drug as having either a drugbank id, a mechanism of action, or an indication.
    val drugMolecule = array_contains(map_keys(col("crossReferences")), "drugbank") ||
      col("indications").isNotNull ||
      col("mechanismsOfAction").isNotNull

    // using left_outer joins as we want to keep all molecules until the filter clause which defines a 'drug' for the
    // purposes of the index.
    val drugDf: DataFrame = moleculeProcessedDf
      .join(indicationProcessedDf, Seq("id"), "left_outer")
      .join(
        mechanismOfActionProcessedDf
          .transform(
            nest(_: DataFrame,
                 List("rows", "uniqueActionTypes", "uniqueTargetTypes"),
                 "mechanismsOfAction")),
        Seq("id"),
        "left_outer"
      )
      .join(targetsAndDiseasesDf, Seq("id"), "left_outer")
      .filter(drugMolecule)
      .transform(addDescription)
      .drop("indications", "mechanismsOfAction")
      .transform(cleanup)

    val dataframesToSave: Map[String, (DataFrame, IOResourceConfig)] = Map(
      "drug" -> (drugDf, outputs.drug),
      "mechanism_of_action" -> (mechanismOfActionProcessedDf, outputs.mechanismOfAction),
      "indication" -> (indicationProcessedDf, outputs.indications)
    )

    val ioResources: IOResources = dataframesToSave mapValues(_._1)
    val saveConfigs: IOResourceConfs = dataframesToSave mapValues(_._2)

    Helpers.writeTo(saveConfigs, ioResources)
  }

  /*
  Final tidying up that aren't business logic but are nice to have for consistent outputs.
   */
  def cleanup(df: DataFrame): DataFrame = {
    Seq("tradeNames", "synonyms").foldLeft(df)((dataF, column) => {
      dataF.withColumn(column, coalesce(col(column), typedLit(Seq.empty)))
    })
  }

}
