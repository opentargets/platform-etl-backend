package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import io.opentargets.etl.backend.spark.{IOResource, IOResourceConfig, IoHelpers}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import io.opentargets.etl.backend.spark.Helpers.{transposeDataframe, unionDataframeDifferentSchema}

/** This step is replacing the process which was previously done in the `data-pipeline` project
  * The next step is to replace this business logic with a new approach.
  */
// This is option/step expression in the config file
object Expression extends LazyLogging {

  /*
     Replace fields name with _
     Map reliability and level to specific set of mapping.
   */
  private def transformNormalTissue(normalTissueDF: DataFrame): DataFrame = {
    val reliabilityMap: Column = (typedLit(
      Map(
        "Supportive" -> true,
        "Uncertain" -> false,
        "Approved" -> true,
        "Supported" -> true,
        "Enhanced" -> true
      )
    ))

    val levelMap: Column = (typedLit(
      Map(
        "Not detected" -> 0,
        "Low" -> 1,
        "Medium" -> 2,
        "High" -> 3,
        "N/A" -> 0,
        "Not representative" -> 0
      )
    ))

    val normalTissueNormDF = normalTissueDF.columns
      .foldLeft(normalTissueDF)((curr, n) => curr.withColumnRenamed(n, n.replaceAll("\\s", "_")))

    normalTissueNormDF
      .filter(col("Level") =!= "N/A")
      .select(
        normalTissueNormDF.col("*"),
        reliabilityMap(col("Reliability")).as("ReliabilityMap"),
        levelMap(col("Level")).as("LevelMap")
      )
  }

  /* Rename the dataframe fields with standard fields.
   */
  private def standardiseBaseline(df: DataFrame): DataFrame = {
    df.withColumnRenamed("key", "Tissue")
      .withColumnRenamed("ID", "Gene")
  }

  /*
    Given three baseline dataframes with the same schema it generates a uniq dataframe per rows
    | Gene, Tissue, rna_val, binned, zscore, unit |
   */
  private def baselineExpressionMaps(
      rnaDF: DataFrame,
      binnedDF: DataFrame,
      zscoreDF: DataFrame
  ): DataFrame = {

    val rnaTransposed = transposeDataframe(rnaDF, Seq("ID")).withColumn("unit", lit("TPM"))
    val binnedTransposed = transposeDataframe(binnedDF, Seq("ID")).withColumn("unit", lit(""))
    val zscoreTransposed = transposeDataframe(zscoreDF, Seq("ID")).withColumn("unit", lit(""))

    val rna = standardiseBaseline(rnaTransposed).withColumnRenamed("val", "rna")
    val binned = standardiseBaseline(binnedTransposed).withColumnRenamed("val", "binned")
    val zscore = standardiseBaseline(zscoreTransposed).withColumnRenamed("val", "zscore")

    val baseExpressions = unionDataframeDifferentSchema(Seq(rna, binned, zscore))

    val baseExpressionGrouped = baseExpressions
      .groupBy("Gene", "Tissue")
      .agg(
        max(col("rna")).as("rna_val"),
        max(col("binned")).as("binned_val"),
        max(col("zscore")).as("zscore_val"),
        first("unit", ignoreNulls = true).as("unit_val")
      )

    baseExpressionGrouped
  }

  /*
    Given tissue and efo info it generate the correlated info.
   */
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

  /*
     It generates the list of valid gene and relative labels.
   */
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
      .withColumnRenamed("Tissue", "TissueDef")
      .select("Gene", "TissueDef")

    val missingLabel = emptyLabels.except(hasLabelFiltered)

    val selectMissingRecords = normalTissueWithLabel
      .join(missingLabel, Seq("Gene"), "right")
      .where(col("TissueDef") === col("Tissue"))
      .withColumnRenamed("TissueDef", "labelDef")

    val validLabels = hasLabels
      .withColumn("labelDef", col("LabelNew"))
      .unionByName(selectMissingRecords)

    validLabels
  }

  /*
    Given the list of gene, tissues it fills the info with rna,level,rscore and unit info.
   */
  private def generateBaselineInfo(
      normalTissueDF: DataFrame,
      baselineExpressionDF: DataFrame
  ): DataFrame = {
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

    val unionByKey = normalTissueKeyDF.join(baselineExpressionKeyDF, Seq("key"), "full")

    val tissueBaselineInfoDF = unionByKey
      .select(
        coalesce(col("GeneNormal"), col("GeneBase")) as "Gene",
        coalesce(col("TissueNormal"), col("TissueBase")) as "Tissue",
        coalesce(col("LevelMap"), lit(-1)) as "LevelMapDef",
        col("Cell_type") as "Cell_type_def",
        coalesce(col("ReliabilityMap"), lit(false)) as "ReliabilityMapDef",
        coalesce(col("rna_val"), lit(0)) as "rna",
        coalesce(col("binned_val"), lit(-1)) as "binned",
        coalesce(col("zscore_val"), lit(-1)) as "zscore",
        coalesce(col("unit_val"), lit("")) as "unit"
      )

    tissueBaselineInfoDF

  }
  /*
     Generate baseline expressions dataframe.
   */
  private def generateExpressions(
      normalTissueDF: DataFrame,
      baselineExpressionDF: DataFrame,
      efoTissueMap: DataFrame
  ): DataFrame = {

    val tissueBaselineInfoDF = generateBaselineInfo(normalTissueDF, baselineExpressionDF)

    val validTissues = selectTissues(tissueBaselineInfoDF, efoTissueMap)
      .drop("efo_code", "labelNew", "label", "name", "expressionId", "tissue_internal_id", "Tissue")
      .distinct

    // Missing tissues. Fixme.
    // val missingTissues = validTissues.filter(col("efoId").isNull).select("labelDef").distinct()

    val protein = validTissues
      .filter(col("efoId").isNotNull)
      .groupBy("Gene", "labelDef", "efoId", "anatomical_systems", "organs")
      .agg(
        max(col("ReliabilityMapDef")).as("reliability"),
        max(col("LevelMapDef")).as("level"),
        struct(
          max(col("rna")).as("value"),
          max(col("zscore")).as("zscore"),
          max(col("binned")).as("level"),
          max(col("unit")).as("unit")
        ).as("rna"),
        collect_list(
          when(
            col("Cell_type_def").isNotNull,
            struct(
              col("Cell_type_def").as("name"),
              col("ReliabilityMapDef").as("reliability"),
              col("LevelMapDef").as("level")
            )
          )
        ).as("cell_type")
      )
      .withColumn(
        "organsValue",
        when(col("organs").isNull, Array.empty[String]).otherwise(col("organs"))
      )
      .withColumn(
        "anatomicalSystems",
        when(col("anatomical_systems").isNull, Array.empty[String])
          .otherwise(col("anatomical_systems"))
      )
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
            struct(
              col("reliability").as("reliability"),
              col("level").as("level"),
              col("cell_type").as("cell_type")
            ).as("protein")
          )
        ).as("tissues")
      )
      .withColumnRenamed("Gene", "id")

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

    val efoTissueMap = efoTissueMapping(
      inputDataFrames("mapwithefos").data,
      inputDataFrames("expressionhierarchy").data
    )

    val baselineExpressionDF = baselineExpressionMaps(
      inputDataFrames("rna").data,
      inputDataFrames("binned").data,
      inputDataFrames("zscore").data
    )

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
