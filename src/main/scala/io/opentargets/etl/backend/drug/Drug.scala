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
  * It incorporates processing which was previously done in the `data-pipeline` project and
  * consolidates all the logic in this class.
  */
object Drug extends Serializable with LazyLogging {

  def apply()(implicit context: ETLSessionContext): Unit = {
    implicit val ss: SparkSession = context.sparkSession

    val drugConfiguration = context.configuration.steps.drug
    val outputs = drugConfiguration.output

    logger.info("Loading raw inputs for Drug beta step.")
    val inputDataFrames = IoHelpers.readFrom(drugConfiguration.input)

    // raw input dataframes
    lazy val probesDf: DataFrame = inputDataFrames("chemical_probes").data
      .filter(col("drugId").isNotNull)
      .select(col("drugId").as("chemicalProbeDrugId"))
      .distinct()
    lazy val moleculeDf: DataFrame = inputDataFrames("chembl_molecule").data
    lazy val mechanismDf: DataFrame = inputDataFrames("chembl_mechanism").data
    lazy val indicationDf: DataFrame = inputDataFrames("chembl_indication").data
    lazy val targetDf: DataFrame = inputDataFrames("chembl_target").data
    lazy val geneDf: DataFrame = inputDataFrames("target_etl").data
    lazy val drugbank2ChemblMap: DataFrame = inputDataFrames("drugbank_to_chembl").data
      .withColumnRenamed("From src:'1'", "id")
      .withColumnRenamed("To src:'2'", "drugbank_id")
    lazy val efoDf: DataFrame = inputDataFrames("disease_etl").data
    lazy val warningRawDf: DataFrame = inputDataFrames("chembl_warning").data

    // processed dataframes
    logger.info("Raw inputs for Drug loaded.")
    logger.info("Processing Drug transformations.")
    val indicationProcessedDf = Indication(indicationDf, efoDf).cache
    val moleculeProcessedDf =
      Molecule(moleculeDf, drugbank2ChemblMap, drugConfiguration.drugExtensions, probesDf)
    val mechanismOfActionProcessedDf: DataFrame =
      MechanismOfAction(mechanismDf, targetDf, geneDf).cache
    val warningsDF = DrugWarning(warningRawDf)
    val linkedTargetDf = computeLinkedTargets(mechanismOfActionProcessedDf)

    logger.whenTraceEnabled {
      val columnString: DataFrame => String = _.columns.mkString("Columns: [", ",", "]")
      logger.trace(s"""Intermediate dataframes:
             Columns:
             \n\t Molecule: ${columnString(moleculeProcessedDf)},
             \n\t Indications: ${columnString(indicationDf)},
             \n\t Mechanisms: ${columnString(mechanismOfActionProcessedDf)},
             Row counts:
             \n\t Molecule: ${moleculeProcessedDf.count},
             \n\t Indications: ${indicationDf.count},
             \n\t Mechanisms: ${mechanismOfActionProcessedDf.count},
             """)
    }

    logger.info(
      "Joining molecules, indications, mechanisms of action, and target and disease linkages."
    )

    // We define a drug as having either a drugbank id, a mechanism of action, an indication, or if it is a chemical probe.
    val isDrugMolecule: Column = array_contains(col("crossReferences.source"), "drugbank") ||
      col("indications").isNotNull ||
      col("mechanismsOfAction").isNotNull ||
      col("chemicalProbeDrugId").isNotNull

    val withdrawnNoticeDf = warningRawDf
      .transform(DrugWarning.processWithdrawnNotices)

    val moleculeWithWithdrawnNoticeDf = moleculeProcessedDf
      .join(withdrawnNoticeDf, Seq("id"), "left")

    // using left_outer joins as we want to keep all molecules until the filter clause which defines a 'drug' for the
    // purposes of the index.
    val drugDf: DataFrame = moleculeWithWithdrawnNoticeDf
      .join(
        indicationProcessedDf
          .select("id", "indications", "linkedDiseases"),
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
      .join(linkedTargetDf, Seq("id"), "left_outer")
      .filter(isDrugMolecule)
      .transform(addDescription)
      .drop("indications", "mechanismsOfAction", "isWithdrawn", "chemicalProbeDrugId")
      .transform(cleanup)

    val dataframesToSave: IOResources = Map(
      "drug" -> IOResource(drugDf, outputs("drug")),
      "mechanism_of_action" -> IOResource(mechanismOfActionProcessedDf,
                                          outputs("mechanism_of_action")
      ),
      "indication" -> IOResource(indicationProcessedDf.drop("linkedDiseases"),
                                 outputs("indications")
      ),
      "drug_warnings" -> IOResource(warningsDF, outputs("warnings"))
    )

    IoHelpers.writeTo(dataframesToSave)
  }

  /*
  Final tidying up that aren't business logic but are nice to have for consistent outputs.
   */
  def cleanup(df: DataFrame): DataFrame =
    // add empty collection as value instead of null values.
    Seq("tradeNames", "synonyms").foldLeft(df) { (dataF, column) =>
      dataF.withColumn(column, coalesce(col(column), typedLit(Seq.empty)))
    }

  /** @param dataFrame
    *   precomputed MOA dataframe
    * @return
    *   dataframe of id, linkedTargets where the id is a ChEMBL ID and `linkedTargets` is a struct
    *   of `row, count` where row is an array of Ensembl Gene IDs.
    */
  def computeLinkedTargets(dataFrame: DataFrame): DataFrame = dataFrame
    .select(explode(col("chemblIds")) as "id", col("targets"))
    .groupBy("id")
    .agg(array_distinct(flatten(collect_list(col("targets")))) as "rows")
    .select(col("id"),
            struct(
              col("rows"),
              size(col("rows")) as "count"
            ) as "linkedTargets"
    )

}
