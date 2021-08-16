package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import io.opentargets.etl.backend.spark.{IOResource, IOResourceConfig, IoHelpers}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import io.opentargets.etl.backend.spark.Helpers.{transposeDataframe, unionDataframeDifferentSchema}

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
      .withColumn("binned_val", lit(-1))
      .withColumn("zscore_val", lit(-1))
      .withColumn("rna_val", lit(0))

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

  private def generateExpressions(normalTissueDF: DataFrame,
                                  baselineExpressionDF: DataFrame,
                                  efoTissueMap: DataFrame): DataFrame = {

    val normalTissuesUnion =
      unionDataframeDifferentSchema(normalTissueDF, baselineExpressionDF)

    val normalTissues = normalTissuesUnion
      .groupBy("Gene", "Tissue")
      .agg(
        max(col("rna_val")).as("rna"),
        max(col("binned_val")).as("binned"),
        max(col("zscore_val")).as("zscore"),
        first("unit_val", ignoreNulls = true).as("unit"),
        first("ReliabilityMap", ignoreNulls = true).as("reliabilityValue"),
        first("LevelMap", ignoreNulls = true).as("levelMapValue"),
        collect_set("Cell_type").as("cellTypes")
      )

    val standardRow = normalTissues.withColumn("singleCellType", explode_outer(col("cellTypes")))

    val normalTissueLabel =
      standardRow.join(efoTissueMap, col("labelNew") === col("Tissue"), "left")

    val normalTissueExpression =
      standardRow.join(efoTissueMap, col("expressionId") === col("Tissue"), "left")

    val hpaExpressionsTransf = normalTissueLabel
      .unionByName(normalTissueExpression)
      .filter(col("labelNew").isNotNull)
      .withColumn("unitVal", when(col("unit").isNull, "").otherwise(col("unit")))
      .withColumn("organsValue",
                  when(col("organs").isNull, Array.empty[String]).otherwise(col("organs")))
      .withColumn("anatomicalSystems",
                  when(col("anatomical_systems").isNull, Array.empty[String])
                    .otherwise(col("anatomical_systems")))
      .drop("label", "name", "efo_code", "expressionId")

    val hpaExpressionAgg = hpaExpressionsTransf
      .groupBy("Gene", "efoId", "labelNew", "organsValue", "anatomicalSystems")
      .agg(
        struct(max(col("rna")).as("value"),
               max(col("zscore")).as("zscore"),
               max(col("binned")).as("level"),
               max(col("unitVal")).as("unit")).as("rna"),
        max(col("reliabilityValue")).as("reliabilitySelected"),
        max(col("levelMapValue")).as("levelSelected"),
        collect_set("singleCellType").as("cellTypes")
      )
      .withColumn("level", when(col("levelSelected").isNull, -1).otherwise(col("levelSelected")))
      .withColumn(
        "reliability",
        when(col("reliabilitySelected").isNull, false).otherwise(col("reliabilitySelected")))
      .drop("levelSelected", "reliabilitySelected")

    val hpaExpressionsProtein =
      hpaExpressionAgg
        .withColumn("cell", explode_outer(col("cellTypes")))
        .groupBy("Gene",
                 "efoId",
                 "labelNew",
                 "organsValue",
                 "anatomicalSystems",
                 "rna",
                 "level",
                 "reliability")
        .agg(
          struct(
            col("reliability").as("reliability"),
            col("level").as("level"),
            collect_list(
              when(col("cell").isNotNull,
                   struct(col("cell").as("name"),
                          col("reliability").as("reliability"),
                          col("level").as("level"))).otherwise(lit(null))
            ).as("cell_type")
          ).as("protein")
        )
        .drop("reliability", "level")

    val hpa = hpaExpressionsProtein
      .groupBy("Gene")
      .agg(
        collect_set(
          struct(
            col("efoId").as("efo_code"),
            col("labelNew").as("label"),
            col("organsValue").as("organs"),
            col("anatomicalSystems").as("anatomical_systems"),
            col("rna").as("rna"),
            col("protein").as("protein")
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
