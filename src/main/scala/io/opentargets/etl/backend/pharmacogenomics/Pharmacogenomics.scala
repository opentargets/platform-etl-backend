package io.opentargets.etl.backend.pharmacogenomics

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import io.opentargets.etl.common.DrugUtils.MapDrugId
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object Pharmacogenomics extends LazyLogging {

  private def getDrugTargetLut(drugs: DataFrame): DataFrame =
    drugs
      .filter(
        col("linkedTargets").isNotNull
          && size(col("linkedTargets.rows")) >= 1
      ) select (
      col("id").alias("drugId"),
      col("linkedTargets.rows").alias("drugTargetIds")
    )

  private def flagIsDirectTarget(variantTarget: Column, drugTargets: Column): Column =
    when(array_contains(drugTargets, variantTarget), true).otherwise(false)

  def apply()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession

    logger.info("Executing Pharmacogenomics step.")

    logger.debug("Reading Pharmacogenomics inputs")
    val mappedInputs = Map(
      "pgx" -> context.configuration.pharmacogenomics.inputs.pgkb,
      "drug" -> context.configuration.pharmacogenomics.inputs.drug
    )

    logger.debug("Processing Pharmacogenomics data")
    val inputDataFrames = IoHelpers.readFrom(mappedInputs)
    val pgxDF = inputDataFrames("pgx").data
    val drugDF = inputDataFrames("drug").data
    val pgxExpanded = pgxDF
      .withColumn("operationalRowId", monotonically_increasing_id())
      .withColumn("drug", explode(col("drugs")))
      .select(col("*"), col("drug.drugFromSource").as("drugFromSource"))
      .drop("drugs")

    logger.debug("Map drug id from molecule")
    val mappedDF = MapDrugId(pgxExpanded, drugDF)

    logger.debug("Get Target Lut")
    val drugTargetLutDF = getDrugTargetLut(drugDF)

    logger.debug("Get Data Enriched")
    val dataEnrichedDF =
      mappedDF
        .join(drugTargetLutDF, Seq("drugId"), "left")
        .withColumn(
          "isDirectTarget",
          flagIsDirectTarget(col("targetFromSourceId"), col("drugTargetIds"))
        )
        .drop("drugTargetIds")
        .distinct()
        // TODO This grouping creates a coupling between the input schema and the operation itself and it could be done
        //  with a more generic approach based on a collection of aggregate expressions
        .groupBy(
          col("operationalRowId"),
          col("datasourceId"),
          col("datasourceVersion"),
          col("datatypeId"),
          col("directionality"),
          col("evidenceLevel"),
          col("genotype"),
          col("genotypeAnnotationText"),
          col("genotypeId"),
          col("haplotypeFromSourceId"),
          col("haplotypeId"),
          col("literature"),
          col("pgxCategory"),
          col("phenotypeFromSourceId"),
          col("phenotypeText"),
          col("studyId"),
          col("targetFromSourceId"),
          col("variantFunctionalConsequenceId"),
          col("variantRsId"),
          col("isDirectTarget")
        )
        .agg(collect_list(struct(col("drugFromSource"), col("drugId"))).as("drugs"))
        .drop("operationalRowId")

    logger.debug("Writing Pharmacogenomics outputs")
    val dataframesToSave: IOResources = Map(
      "pgx" -> IOResource(dataEnrichedDF, context.configuration.pharmacogenomics.outputs)
    )

    IoHelpers.writeTo(dataframesToSave)
  }

}
