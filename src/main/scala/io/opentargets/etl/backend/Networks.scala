package io.opentargets.etl.backend

import org.apache.spark.SparkConf
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.typesafe.config.Config
import io.opentargets.etl.backend.SparkHelpers.IOResourceConfig

object NetworksHelpers {
  implicit class AggregationHelpers(df: DataFrame)(implicit ss: SparkSession) {
    import Configuration._
    import ss.implicits._

    def transformRnacentral: DataFrame = {
      df.withColumnRenamed("_c0", "mapped_id")
        .withColumnRenamed("_c5", "gene_id")
        .select("gene_id", "mapped_id")
    }

    def transformHumanMapping: DataFrame = {
      df.filter(col("_c1") === "Ensembl")
        .groupBy("_c2")
        .agg(collect_list("_c0").alias("mapping_list"))
        .withColumnRenamed("_c2", "id")
        .select("id", "mapping_list")
    }

    def createMappingInfoDF(rnaCentral: DataFrame, humanMapping: DataFrame): DataFrame = {
      val targets = (
        df.withColumn("proteins", col("proteinAnnotations.accessions")).select("id", "proteins"))
      val humanMappingDF = humanMapping.transformHumanMapping

      val mappingInfoDF = targets
        .join(humanMappingDF, Seq("id"), "left")
        .withColumn("mapped_id_list", array_union(col("proteins"), col("mapping_list")))
        .select("id", "mapped_id_list")
        .distinct
        .withColumnRenamed("id", "gene_id")

      val mappingInfoExplodeDF = mappingInfoDF.withColumn("mapped_id", explode(col("mapped_id_list"))).drop("mapped_id_list")

      mappingInfoExplodeDF.printSchema

      mappingInfoExplodeDF

    }

    // ETL for intact info.
    def generateInteractions(mappingInfo: DataFrame): DataFrame = {

      val intactInfoDF = df
        .withColumn("intA", col("interactorA.id"))
        .withColumn("intA_source", col("interactorA.id_source"))
        .withColumn(
          "intB",
          when(col("interactorB.id").isNull, col("interactorA.id")).otherwise(col("interactorB.id"))
        )
        .withColumn("intB_source", col("interactorB.id_source"))
        .withColumnRenamed("source_info", "interactionResources")
        .withColumn("interactionScore", col("interaction.interaction_score"))
        .withColumn(
          "causalInteraction",
          when(col("interaction.causal_interaction").isNull, false).otherwise(
            col("interaction.causal_interaction").cast("boolean")
          )
        )
        .withColumn("speciesA", col("interactorA.organism"))
        .withColumn("speciesB", col("interactorB.organism"))
        .withColumn("intABiologicalRole", col("interactorA.biological_role"))
        .withColumn("intBBiologicalRole", col("interactorB.biological_role"))
        .withColumn("evidences", explode(col("interaction.evidence")))
        .selectExpr(
          "intA",
          "intA_source",
          "speciesA",
          "intB",
          "intB_source",
          "speciesB",
          "interactionResources",
          "interactionScore",
          "causalInteraction",
          "evidences",
          "intABiologicalRole",
          "intBBiologicalRole"
        )

      val mappingLeftDF = intactInfoDF
        .join(mappingInfo,split(col("intA"), "_").getItem(0) === col("mapped_id"), "left")
        .withColumn("targetA", when(col("gene_id").isNull, col("intA")).otherwise(col("gene_id")))
        .drop("gene_id", "mappingInfo.mapping_id")

      mappingLeftDF.printSchema

      val mappingDF = mappingLeftDF
        .join(mappingInfo.alias("mapping"),split(col("intB"), "_").getItem(0)  === col("mapping.mapped_id"), "left")
        .withColumn("targetB", when(col("gene_id").isNull, col("intB")).otherwise(col("gene_id")))
        .drop("gene_id", "mapping.mapped_id")

      mappingDF.printSchema
      mappingDF
    }


    def getAUnmatch: DataFrame = {
       df.filter(col("targetA") === col("intA")).filter(col("speciesA.taxon_id") === 9606)
    }

    def getBUnmatch: DataFrame = {
       df.filter(col("targetB") === col("intB")).filter(col("speciesB.taxon_id") === 9606)
    }

    def getUnmatch: DataFrame = {
       df
       .filter("((targetA = intA) and (speciesA.taxon_id = 9606)) or ((targetB = intB) and (speciesB.taxon_id = 9606))")
       .select("targetA","targetB")

    }

    def selectFields: DataFrame = {
      df.selectExpr(
        "targetA",
        "intA",
        "intA_source",
        "speciesA",
        "targetB",
        "intB",
        "intB_source",
        "speciesB",
        "interactionResources",
        "interactionScore",
        "causalInteraction",
        "evidences.*",
        "intABiologicalRole",
        "intBBiologicalRole"
      )

    }
    def generateInteractionsAgg: DataFrame = {

      //).withColumn("intBTargetIDs", when(col("intBTargetID").isNull, array(explode(col("intBTargetIDs"))))
      //  .otherwise(col("intBTargetIDs")))

      val interactionsAgg = df
        .groupBy(
          "interactionResources.source_database",
          "targetA",
          "intABiologicalRole",
          "targetB",
          "intBBiologicalRole"
        )
        .agg(
          count(col("evidences")).alias("sizeEvidences"),
          collect_set(col("intA")).alias("intA_IDs"),
          collect_set(col("intB")).alias("intB_IDs"),
          collect_list(
            struct(
              col("interactionScore"),
              col("targetA"),
              col("intABiologicalRole"),
              col("targetB"),
              col("intBBiologicalRole")
            )
          ).as("scoring")
        )
        .withColumn("sizeScoring", size(col("scoring")))

      interactionsAgg
    }
  }
}

// This is option/step otnetwork in the config file
object Networks extends LazyLogging {

  def compute()(implicit context: ETLSessionContext): Map[String, DataFrame] = {
    implicit val ss = context.sparkSession
    import ss.implicits._
    import NetworksHelpers._

    val common = context.configuration.common
    val mappedInputs = Map(
      "rnacentral" -> IOResourceConfig(
        common.inputs.networks.rnacentral.format,
        common.inputs.networks.rnacentral.path
      ),
      "humanmapping" -> IOResourceConfig(
        common.inputs.networks.humanmapping.format,
        common.inputs.networks.humanmapping.path
      ),
      "interactions" -> IOResourceConfig(
        common.inputs.networks.interactions.format,
        common.inputs.networks.interactions.path
      )
    )

    val targets = Target.compute()
    val inputDataFrame = SparkHelpers.readFrom(mappedInputs)

    val mappingDF = targets.createMappingInfoDF(
      inputDataFrame("rnacentral"),
      inputDataFrame("humanmapping")
    )

    val interactionDF = inputDataFrame("interactions").generateInteractions(mappingDF)
    val interactionEvidences = interactionDF.selectFields
    val aggInteractionDF = interactionDF.generateInteractionsAgg

    // TODO CINZIA WRITE IT DOWN TO A JSONLINES OUTPUT TO SHARE WITH DATA TEAM
    Map(
      "interactionEvidences" -> interactionEvidences,
      "interactions" -> aggInteractionDF,
      "interactionUnmatch" -> interactionEvidences.getUnmatch
    //  "interactionAUnmatch" -> interactionEvidences.getAUnmatch,
    //  "interactionBUnmatch" -> interactionEvidences.getBUnmatch
    )
  }

  def apply()(implicit context: ETLSessionContext) = {
    implicit val ss = context.sparkSession
    import ss.implicits._

    val otnetworksDF = compute()

    // TODO THIS NEEDS MORE REFACTORING WORK AS IT CAN BE SIMPLIFIED
    val outputConfs = otnetworksDF.keys
      .map(name =>
        name -> IOResourceConfig(
          context.configuration.common.outputFormat,
          context.configuration.common.output + s"/$name"
        )
      )
      .toMap

    SparkHelpers.writeTo(outputConfs, otnetworksDF)
  }
}
