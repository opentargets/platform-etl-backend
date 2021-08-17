package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import io.opentargets.etl.backend.spark.{IOResource, IOResourceConfig, IoHelpers}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import io.opentargets.etl.backend.spark.Helpers.{
  transposeDataframe,
  unionDataframeDifferentSchema,
  validateDF
}
import org.scalacheck.Prop.True

// This is option/step expression in the config file
object Expression extends LazyLogging {

  private def transformNormalTissue(normalTissueDF: DataFrame): DataFrame = {
    val reliabilityMap: Column = (typedLit(
      Map(
        "Supportive" -> true,
        "Uncertain" -> false,
        "Approved" -> true,
        "Supported" -> true,
        "Enhanced" -> true
      )))

    val levelMap: Column = (typedLit(
      Map(
        "Not detected" -> 0,
        "Low" -> 1,
        "Medium" -> 2,
        "High" -> 3,
        "N/A" -> 0,
        "Not representative" -> 0
      )))

    val normalTissueNormDF = normalTissueDF.columns
      .foldLeft(normalTissueDF)((curr, n) => curr.withColumnRenamed(n, n.replaceAll("\\s", "_")))

    normalTissueNormDF
      .filter(col("Level") =!= "N/A")
      .select(normalTissueNormDF.col("*"),
              reliabilityMap(col("Reliability")).as("ReliabilityMap"),
              levelMap(col("Level")).as("LevelMap"))
    //.withColumn("binned_val", lit(-1))
    //.withColumn("zscore_val", lit(-1))
    //.withColumn("rna_val", lit(0))

  }

  private def standardiseBaseline(df: DataFrame): DataFrame = {
    df.withColumnRenamed("key", "Tissue")
      .withColumnRenamed("ID", "Gene")
  }

  private def baselineExpressionMaps(rnaDF: DataFrame,
                                     binnedDF: DataFrame,
                                     zscoreDF: DataFrame): DataFrame = {

    val rnaTransposed = transposeDataframe(rnaDF, Seq("ID")).withColumn("unit", lit("TPM"))
    val binnedTransposed = transposeDataframe(binnedDF, Seq("ID")).withColumn("unit", lit(""))
    val zscoreTransposed = transposeDataframe(zscoreDF, Seq("ID")).withColumn("unit", lit(""))

    val rna = standardiseBaseline(rnaTransposed).withColumnRenamed("val", "rna")
    val binned = standardiseBaseline(binnedTransposed).withColumnRenamed("val", "binned")
    val zscore = standardiseBaseline(zscoreTransposed).withColumnRenamed("val", "zscore")

    val baseExpressions =
      unionDataframeDifferentSchema(unionDataframeDifferentSchema(rna, binned), zscore)

    val baseExpressionGrouped = baseExpressions
      .groupBy("Gene", "Tissue")
      .agg(max(col("rna")).as("rna_val"),
           max(col("binned")).as("binned_val"),
           max(col("zscore")).as("zscore_val"),
           first("unit", ignoreNulls = true).as("unit_val"))

    baseExpressionGrouped
  }

  private def efoTissueMapping(mapEfos: DataFrame, expressions: DataFrame): DataFrame = {
    val expressionsRenamed = expressions
      .withColumnRenamed("_c0", "expressionId")
      .withColumnRenamed("_c1", "name")

    val mapEfosRenamed = mapEfos.withColumnRenamed("tissue_id", "tissue_internal_id")

    val mappedInfo = mapEfosRenamed
      .join(expressionsRenamed, col("name") === col("tissue_internal_id"), "full")
      .withColumn("efoId", when(col("efo_code").isNull, col("name")).otherwise(col("efo_code")))
      .withColumn("labelNew", when(col("label").isNull, col("name")).otherwise(col("label")))

    mappedInfo
  }

  private def selectTissues(tissues: DataFrame, efoTissueMap: DataFrame) = {
    val normalTissueLabel =
      tissues.join(efoTissueMap, col("labelNew") === col("Tissue"), "left")
    val normalTissueExpression =
      tissues.join(efoTissueMap, col("expressionId") === col("Tissue"), "left")

    val normalTissueWithLabel = normalTissueLabel.unionByName(normalTissueExpression)

    val emptyLabels = normalTissueWithLabel
      .filter(col("labelNew").isNull)
      .withColumn("TissueDef", col("Tissue"))
      .select("Gene", "TissueDef")

    val hasLabels = normalTissueWithLabel
      .filter(col("labelNew").isNotNull)

    val hasLabelFiltered = hasLabels
      .withColumn("TissueDef", col("Tissue"))
      .select("Gene", "TissueDef")

    val missingLabel = emptyLabels.except(hasLabelFiltered)

    val selectMissingRecords = normalTissueWithLabel
      .join(missingLabel, Seq("Gene"), "right")
      .where(col("TissueDef") === col("Tissue"))
      .withColumn("labelDef", col("TissueDef"))
      .drop("TissueDef")

    val validLabels = hasLabels
      .withColumn("labelDef", col("LabelNew"))
      .unionByName(selectMissingRecords)

    validLabels
  }

  private def generateExpressions(normalTissueDF: DataFrame,
                                  baselineExpressionDF: DataFrame,
                                  efoTissueMap: DataFrame): DataFrame = {

    val normalTissueKeyDF =
      normalTissueDF
        .withColumn("key", concat(col("Gene"), lit('-'), col("Tissue")))
        .withColumnRenamed("Gene", "GeneNormal")
        .withColumnRenamed("Tissue", "TissueNormal")

    val baselineExpressionKeyDF =
      baselineExpressionDF
        .withColumn("key", concat(col("Gene"), lit('-'), col("Tissue")))
        .withColumnRenamed("Gene", "GeneBase")
        .withColumnRenamed("Tissue", "TissueBase")

    val mix = normalTissueKeyDF.join(baselineExpressionKeyDF, Seq("key"), "full")

    mix.filter(col("GeneBase") === "ENSG00000000003")

    /* .filter(col("GeneBase") === "ENSG00000000003")
      .filter(col("Gene") === "ENSG00000090520")
      .filter(col("Tissue") === "cerebral cortex")
     */

    val normalTissuesUnion = mix
      .withColumn("Gene",
                  when(col("GeneNormal").isNull, col("GeneBase")).otherwise(col("GeneNormal")))
      .withColumn(
        "Tissue",
        when(col("TissueNormal").isNull, col("TissueBase")).otherwise(col("TissueNormal")))
      .withColumn("LevelMapDef", when(col("LevelMap").isNull, -1).otherwise(col("LevelMap")))
      .withColumn("ReliabilityMapDef",
                  when(col("ReliabilityMap").isNull, false).otherwise(col("ReliabilityMap")))
      .withColumn("rna", when(col("rna_val").isNull, 0).otherwise(col("rna_val")))
      .withColumn("binned", when(col("binned_val").isNull, -1).otherwise(col("binned_val")))
      .withColumn("zscore", when(col("zscore_val").isNull, -1).otherwise(col("zscore_val")))
      .withColumn("unit", when(col("unit_val").isNull, "").otherwise(col("unit_val")))
      .withColumn("Cell_type_def", when(col("Cell_type").isNull, null).otherwise(col("Cell_type")))
      .select("Gene",
              "Tissue",
              "Cell_type_def",
              "LevelMapDef",
              "ReliabilityMapDef",
              "rna",
              "binned",
              "zscore",
              "unit")

    val validTissues = selectTissues(normalTissuesUnion, efoTissueMap)
      .drop("efo_code", "labelNew", "label", "name", "expressionId", "tissue_internal_id", "Tissue")
      .distinct

    val protein = validTissues
      .groupBy("Gene", "labelDef", "efoId", "anatomical_systems", "organs")
      .agg(
        first(col("ReliabilityMapDef"), ignoreNulls = true).as("reliability"),
        max(col("LevelMapDef")).as("level"),
        struct(max(col("rna")).as("value"),
               max(col("zscore")).as("zscore"),
               max(col("binned")).as("level"),
               max(col("unit")).as("unit")).as("rna"),
        collect_list(
          when(col("Cell_type_def").isNotNull,
               struct(col("Cell_type_def").as("name"),
                      col("ReliabilityMapDef").as("reliability"),
                      col("LevelMapDef").as("level")))
        ).as("cell_type")
      )
      .withColumn("organsValue",
                  when(col("organs").isNull, Array.empty[String]).otherwise(col("organs")))
      .withColumn("anatomicalSystems",
                  when(col("anatomical_systems").isNull, Array.empty[String])
                    .otherwise(col("anatomical_systems")))
      .drop("organs", "anatomical_systems")

    val hpa = protein
      .groupBy("Gene")
      .agg(
        collect_set(
          struct(
            col("efoId").as("efo_code"),
            col("labelDef").as("label"),
            col("organsValue").as("organs"),
            col("anatomicalSystems").as("anatomical_systems"),
            col("rna").as("rna"),
            struct(col("reliability").as("reliability"),
                   col("level").as("level"),
                   col("cell_type").as("cell_type")).as("protein")
          )).as("tissues")
      )
      .withColumnRenamed("Gene", "gene")

    hpa

  }

  // Public because it used by connection.scala
  def compute()(implicit context: ETLSessionContext): DataFrame = {
    implicit val ss: SparkSession = context.sparkSession

    val HPAConfiguration = context.configuration.expression

    logger.info("Loading raw inputs for Base Expression step.")
    val mappedInputs = Map(
      "tissues" -> HPAConfiguration.tissues,
      "rna" -> HPAConfiguration.rna,
      "zscore" -> HPAConfiguration.zscore,
      "binned" -> HPAConfiguration.binned,
      "mapwithefos" -> HPAConfiguration.efomap,
      "expressionhierarchy" -> HPAConfiguration.exprhierarchy
    )

    val inputDataFrames = IoHelpers.readFrom(mappedInputs)

    val normalTissueDF = transformNormalTissue(inputDataFrames("tissues").data)
    val efoTissueMap = efoTissueMapping(inputDataFrames("mapwithefos").data,
                                        inputDataFrames("expressionhierarchy").data)

    val baselineExpressionDF = baselineExpressionMaps(inputDataFrames("rna").data,
                                                      inputDataFrames("binned").data,
                                                      inputDataFrames("zscore").data)

    val expressionDF = generateExpressions(normalTissueDF, baselineExpressionDF, efoTissueMap)
    expressionDF

  }

  def apply()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession

    logger.info("transform Baseline Expression dataset")
    val dataframesToSave = compute()

    logger.info(s"write to ${context.configuration.common.output}/baselineExpression")
    val outputs = Map(
      "baselineExpression" -> IOResource(dataframesToSave, context.configuration.expression.output)
    )

    IoHelpers.writeTo(outputs)
  }
}
