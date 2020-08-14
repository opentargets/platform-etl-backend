package io.opentargets.etl.backend

import org.apache.spark.SparkConf
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.typesafe.config.Config
import io.opentargets.etl.backend.SparkHelpers.IOResourceConfig

object OTNetworksHelpers {
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
        .withColumnRenamed("_c0", "mapped_id")
        .withColumnRenamed("_c2", "gene_id")
        .select("gene_id", "mapped_id")
    }

    def createMappingInfoDF(rnaCentral: DataFrame, humanMapping: DataFrame): DataFrame = {
      val ensembl = df.withColumnRenamed("protein_id", "mapped_id")
      val rnacentralDF = rnaCentral.transformRnacentral
      val humanMappingDF = humanMapping.transformHumanMapping

      val humanMappingDFValid = ensembl
        .select("gene_id")
        .join(humanMappingDF, Seq("gene_id"), "left")
        .filter(col("mapped_id").isNotNull)
        .drop(humanMappingDF("gene_id"))

      val rnacentralDFValid = ensembl
        .select("gene_id")
        .join(rnacentralDF, Seq("gene_id"), "left")
        .filter(col("mapped_id").isNotNull)
        .drop(rnacentralDF("gene_id"))

      // Create an unique dataframe with gene_id and mapping.
      val mappedInfo = ensembl.union(rnacentralDFValid).union(humanMappingDFValid)
      mappedInfo.printSchema()
      mappedInfo.show(10)
      mappedInfo

    }

    // ETL for intact info.
    def generateInteractions(mappingInfo: DataFrame): DataFrame = {
      val intactInfoDF = df
        .withColumn("intA_sourceID", col("interactorA.id"))
        .withColumn("intA_source", col("interactorA.id_source"))
        .withColumn("intB_sourceID", col("interactorB.id"))
        .withColumn("intB_source", col("interactorB.id_source"))
        .withColumnRenamed("source_info", "interactionResources")
        .withColumn("interactionScore", col("interaction.interaction_score"))
        .withColumn("causalInteraction", col("interaction.causal_interaction"))
        .withColumn("speciesA", col("interactorA.organism"))
        .withColumn("speciesB", col("interactorB.organism"))
        .withColumn("intABiologicalRole", col("interactorA.biological_role"))
        .withColumn("intBBiologicalRole", col("interactorB.biological_role"))
        .withColumn("evidences", explode(col("interaction.evidence")))
        .select(
          "intA_sourceID",
          "intA_source",
          "speciesA",
          "intB_sourceID",
          "intB_source",
          "speciesB",
          "interactionResources",
          "interactionScore",
          "causalInteraction",
          "evidences",
          "intABiologicalRole",
          "intBBiologicalRole"
        )

      mappingInfo.printSchema()

      // Todo:
      // Remove _ or -
      // interactorBId can be null. So it is an identify. InteractorB = InteractionA
      // Check if Ids are already EnsemblId -> targetA and targetB

      // rnaCentral manipulation. Some ID as _9606 added to the id. Remove them..filter(col("targetA").isNotNull())
      val mappingLeftDF = intactInfoDF
        .join(
          mappingInfo,
          split(col("intA_sourceID"), "_").getItem(0) === mappingInfo("mapped_id"),
          "left"
        )
        .withColumnRenamed("gene_id", "intATarget")

      val mappingDF = mappingLeftDF
        .join(
          mappingInfo.alias("mapping"),
          split(col("intB_sourceID"), "_").getItem(0) === col("mapping.mapped_id"),
          "left"
        )
        .withColumnRenamed("gene_id", "intBTarget")
        .select(
          "intATarget",
          "intA_sourceID",
          "intA_source",
          "speciesA",
          "intBTarget",
          "intB_sourceID",
          "intB_source",
          "speciesB",
          "interactionResources",
          "interactionScore",
          "causalInteraction",
          "evidences",
          "intABiologicalRole",
          "intBBiologicalRole"
        )

      return mappingDF
    }
  }
}

// This is option/step otnetwork in the config file
object OTNetworks extends LazyLogging {

  def compute()(implicit context: ETLSessionContext): DataFrame = {
    implicit val ss = context.sparkSession
    import ss.implicits._
    import OTNetworksHelpers._

    val common = context.configuration.common
    val mappedInputs = Map(
      "ensemblProtein" -> IOResourceConfig(
        common.inputs.otnetworks.ensemblprotein.format,
        common.inputs.otnetworks.ensemblprotein.path
      ),
      "rnacentral" -> IOResourceConfig(
        common.inputs.otnetworks.rnacentral.format,
        common.inputs.otnetworks.rnacentral.path,
        common.inputs.otnetworks.rnacentral.delimiter,
        common.inputs.otnetworks.rnacentral.header
      ),
      "humanmapping" -> IOResourceConfig(
        common.inputs.otnetworks.humanmapping.format,
        common.inputs.otnetworks.humanmapping.path,
        common.inputs.otnetworks.humanmapping.delimiter,
        common.inputs.otnetworks.humanmapping.header
      ),
      "interactions" -> IOResourceConfig(
        common.inputs.otnetworks.interactions.format,
        common.inputs.otnetworks.interactions.path
      )
    )

    val inputDataFrame = SparkHelpers.readFrom(mappedInputs)
    val mappingDF = inputDataFrame("ensemblProtein").createMappingInfoDF(
      inputDataFrame("rnacentral"),
      inputDataFrame("humanmapping")
    )

    val interactionDF = inputDataFrame("interactions").generateInteractions(mappingDF)

    interactionDF
  }

  def apply()(implicit context: ETLSessionContext) = {
    implicit val ss = context.sparkSession
    import ss.implicits._

    val otnetworksDF = compute()

    val mappingUniqueInteraction = otnetworksDF
      .select(
        "intATarget",
        "intBTarget",
        "intABiologicalRole",
        "intBBiologicalRole",
        "interactionResources",
        "interactionScore"
      ).distinct()
      
    val outputs = Seq("otnetworks")
    // TODO THIS NEEDS MORE REFACTORING WORK AS IT CAN BE SIMPLIFIED
    val outputConfs = outputs
      .map(name =>
        name -> IOResourceConfig(
          context.configuration.common.outputFormat,
          context.configuration.common.output + s"/$name"
        )
      )
      .toMap

    val outputDFs = (outputs zip Seq(otnetworksDF)).toMap
    SparkHelpers.writeTo(outputConfs, outputDFs)
  }
}
