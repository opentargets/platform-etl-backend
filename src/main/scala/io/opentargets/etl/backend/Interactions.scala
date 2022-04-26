package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.InteractionsHelpers.logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.typesafe.config.Config
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import spark.{Helpers, IOResource, IoHelpers}
import io.opentargets.etl.backend.stringProtein.StringProtein
import org.apache.spark.sql.functions.udf

object InteractionsHelpers extends LazyLogging {
  implicit class AggregationHelpers(df: DataFrame)(implicit ss: SparkSession) {
    import Configuration._

    /** from the RNACentral file generates a dataframe with gene_id, mapped_id Columns.
      * Every single mapping file has to return a common dataframe schema in order to easily union them.
      * @return a DataFrame (gene_id, mapped_id)
      */
    def transformRnacentral: DataFrame = {
      df.withColumnRenamed("_c0", "mapped_id")
        .withColumnRenamed("_c5", "gene_id")
        .select("gene_id", "mapped_id")
    }

    /** from Human Mapping file generates a dataframe with gene_id, mapped_id Columns.
      * Every single mapping file has to return a common dataframe schema in order to easily union them.
      * @return a DataFrame (gene_id, mapped_id)
      */
    def transformHumanMapping: DataFrame = {
      df.filter(col("_c1") === "Ensembl")
        .groupBy("_c2")
        .agg(collect_list("_c0").alias("mapping_list"))
        .withColumnRenamed("_c2", "id")
        .withColumn("mapping_list", coalesce(col("mapping_list"), array()))
        .select("id", "mapping_list")
    }

    /** from Human mapping it extract links between Gene Name and gene_id and other id.
      * @return a DataFrame (gene_id, mapped_id)
      */
    def transformGeneIds(humanMapping: DataFrame): DataFrame = {
      val genes = humanMapping
        .filter(col("_c1") === "Gene_Name")
        .groupBy("_c2")
        .agg(collect_list("_c0").alias("mapping_list"))

      val geneIds = genes.withColumn("mapped_id", explode(col("mapping_list"))).drop("mapping_list")

      val combinationInfo = geneIds.join(df, Seq("mapped_id"), "left")
      val mapped = combinationInfo.filter(col("gene_id").isNotNull).drop("mapped_id").distinct
      val mapped_not = combinationInfo.filter(col("gene_id").isNull).drop("gene_id")
      val geneMapping = mapped_not.join(mapped, Seq("_c2")).select("gene_id", "mapped_id").distinct

      geneMapping
    }

    /** generate the mapping gene_id, mapped_id. It will be used to map the intact entries.
      * df is the implicit dataframe. Target is computed from another module.
      * @param rnaCentral rna central resource
      * @param humanMapping human mapping resource
      * @return a DataFrame
      */
    def generateMapping(rnaCentral: DataFrame, humanMapping: DataFrame): DataFrame = {
      val targetsProteins = df
        .withColumn("proteins", coalesce(col("proteinIds.id"), array()))
        .select("id", "proteins")
      val targetHGNC = df
        .select(
          col("id"),
          filter(col("dbXRefs"), col => { col.getField("source") === "HGNC" }) as "h"
        )
        .withColumn("mapped_id", explode(col("h.id")))
        .select(col("id") as "gene_id", concat(lit("HGNC:"), col("mapped_id")) as "mapped_id")
      val humanMappingDF = humanMapping.transformHumanMapping
      val rnaMappingDF = rnaCentral.transformRnacentral

      val mappingHuman = (
        targetsProteins
          .join(humanMappingDF, Seq("id"), "left")
          .withColumn(
            "mapped_id_list",
            when(col("mapping_list").isNull, col("proteins"))
              .otherwise(array_union(col("proteins"), col("mapping_list")))
          )
          .select("id", "mapped_id_list")
          .distinct
          .withColumnRenamed(
            "id",
            "gene_id"
          )
      )

      val mappingExplode =
        mappingHuman.withColumn("mapped_id", explode(col("mapped_id_list"))).drop("mapped_id_list")

      val mapGeneIds = mappingExplode.transformGeneIds(humanMapping)
      val mapping = mappingExplode.union(rnaMappingDF).union(targetHGNC).union(mapGeneIds)

      mapping.distinct

    }

    /** generate the interactions from Intact resource
      * df is the implicit dataframe: Intact dataframe
      * @param targets Dataframe with list of ensembl_id, protein_id
      * @param rnacentral Dataframe with the rna_id, ensembl_id
      * @param humanmapping dataframe with human_id, ensembl_id
      * @return a DataFrame
      */
    def generateIntacts(
        targets: DataFrame,
        rnacentral: DataFrame,
        humanmapping: DataFrame
    ): DataFrame = {
      val mappingDF = targets.generateMapping(rnacentral, humanmapping)
      val intactInteractions = df.generateInteractions(mappingDF)
      intactInteractions
    }

    /** generate the interactions from Strings resource.
      * df is the implicit dataframe: Strings dataframe
      * @param ensproteins Dataframe with the protein_id, ensembl_id
      * @return a DataFrame
      */
    def generateStrings(ensproteins: DataFrame): DataFrame = {
      val mapping = ensproteins.withColumnRenamed("protein_id", "mapped_id").distinct
      val stringInteractions = df
        .generateInteractions(mapping)
        .filter(col("evidences.evidence_score") > 0)

      stringInteractions
    }

    // Common procedure to transform the Dataframes in common Interaction entries.

    /** generate a string truncated at - or _ char.
      * Eg. "URS123-2_992   return URS123"
      * @param s string with possible _, -, .  or spaces chars
      * @return a string
      */
    val getCodeFcn = udf { s: String =>
      s.trim.split("_")(0).split("-")(0)
    }

    /** generate the interactions from a common Dataframe schema
      * For intact resource (intact,reactomea and signor) we swap (A, B) and add to the dataframe
      * String data is symetrical by definition so no swap is required
      * @param mappingInfo Dataframe with mapping_id, ensembl_id
      * @return a DataFrame
      */
    def generateInteractions(mappingInfo: DataFrame): DataFrame = {

      val interactions = df
        .withColumn(
          "intB",
          when(col("interactorB.id").isNull, col("interactorA.id")).otherwise(col("interactorB.id"))
        )
        .withColumn(
          "intB_source",
          when(col("interactorB.id_source").isNull, col("interactorA.id_source")).otherwise(
            col("interactorB.id_source")
          )
        )
        .withColumn(
          "speciesB",
          when(col("interactorB.organism").isNull, col("interactorA.organism")).otherwise(
            col("interactorB.organism")
          )
        )
        .withColumn(
          "intBBiologicalRole",
          when(col("interactorB.biological_role").isNull, col("interactorA.biological_role"))
            .otherwise(col("interactorB.biological_role"))
        )
        .withColumn(
          "interactionScore",
          when(
            col("interaction.interaction_score") > 1,
            col("interaction.interaction_score") / 1000
          ).otherwise(col("interaction.interaction_score"))
        )
        .selectExpr(
          "interactorA.id as intA",
          "interactorA.id_source as intA_source",
          "interactorA.organism as speciesA",
          "interactorA.biological_role as intABiologicalRole",
          "intB",
          "intB_source",
          "speciesB",
          "intBBiologicalRole",
          "source_info.source_database as sourceDatabase",
          "source_info as interactionResources",
          "interaction.evidence as evidencesList",
          "interactionScore"
        )

      val interactionMapLeft = interactions
        .join(mappingInfo, getCodeFcn(col("intA")) === col("mapped_id"), "left")
        .withColumn("targetA", when(col("gene_id").isNull, lit(null)).otherwise(col("gene_id")))
        .drop("gene_id", "mapped_id")

      val interactionMapped = interactionMapLeft
        .join(
          mappingInfo.alias("mapping"),
          getCodeFcn(col("intB")) === col("mapping.mapped_id"),
          "left"
        )
        .withColumn("targetB", when(col("gene_id").isNull, lit(null)).otherwise(col("gene_id")))
        .drop("gene_id", "mapping.mapped_id")

      //  Reverse Value and UNION for specific case
      val lookup = Map(
        "targetA" -> "targetB",
        "intA" -> "intB",
        "intA_source" -> "intB_source",
        "speciesA" -> "speciesB",
        "intABiologicalRole" -> "intBBiologicalRole",
        "targetB" -> "targetA",
        "intB" -> "intA",
        "intB_source" -> "intA_source",
        "speciesB" -> "speciesA",
        "intBBiologicalRole" -> "intABiologicalRole"
      )

      // For intact resource (intact,reactomea and signor) swap (A, B) and add to the dataframe
      val reverseInteractions = interactionMapped
        .filter(col("sourceDatabase").isin(List("reactome", "intact", "signor"): _*))
        .select(interactionMapped.columns.map(c => col(c).as(lookup.getOrElse(c, c))): _*)

      val fullInteractions = interactionMapped.unionByName(reverseInteractions)

      logger.info("fullInteractions done.")

      val interactionEvidences = fullInteractions
        .withColumn("evidences", explode(col("evidencesList")))
        .drop("evidencesList", "sourceDatabase")

      interactionEvidences
    }

    /** select the fields for the intex interaction_evidences
      * @return a DataFrame
      */
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
        "evidences.*",
        "intABiologicalRole",
        "intBBiologicalRole"
      )

    }

    /** aggragate and count TargetA and Target B.
      * This dataframe generates the info stored in the index interactions
      * @return a DataFrame
      */
    def generateInteractionsAgg: DataFrame = {

      val interactionsAggregated = df
        .groupBy(
          "interactionResources.source_database",
          "targetA",
          "intA",
          "intABiologicalRole",
          "targetB",
          "intB",
          "intBBiologicalRole",
          "speciesA",
          "speciesB"
        )
        .agg(
          count(col("evidences")).alias("count"),
          first(col("interactionScore")).as("scoring")
        )
        .withColumnRenamed("source_database", "sourceDatabase")

      interactionsAggregated
    }

    /** Union of the aggregations of intact and strings
      * df is the implicit dataframe: Intact dataframe
      * @param interactionStrings dataframe with string info
      * @return a DataFrame
      */
    def interactionAggreation(interactionStrings: DataFrame): DataFrame = {
      val intactAggregation = df.generateInteractionsAgg
      val stringsAggregation = interactionStrings.generateInteractionsAgg
      val interactionAggreation = intactAggregation.unionByName(stringsAggregation)

      interactionAggreation

    }

    /** Union of the evidences of intact and strings
      * df is the implicit dataframe: Intact dataframe
      * @param stringInteractions dataframe with strings info
      * @return a DataFrame
      */
    def generateEvidences(stringInteractions: DataFrame): DataFrame = {
      val intactInteractionEvidences = df.selectFields
      val stringInteractionEvidences =
        stringInteractions.selectFields.withColumn("evidence_score", col("evidence_score") / 1000)

      val interactionEvidences = Helpers.unionDataframeDifferentSchema(
        stringInteractionEvidences,
        intactInteractionEvidences
      )

      val interationEvidencesLowerCamel = Helpers.snakeToLowerCamelSchema(interactionEvidences)
      interationEvidencesLowerCamel
    }
  }
}

// This is option/step interaction in the config file
object Interactions extends LazyLogging {

  /** @param dataframes with targetA and/or targetB with null value.
    * @return a DataFrame with the interaction_id unmatched
    */
  def getUnmatch(intact: DataFrame, string: DataFrame)(implicit ss: SparkSession): DataFrame = {

    val intactMissing = intact
      .filter(col("targetB").isNull && col("speciesB.taxon_id") === 9606)
      .select("intB")

    val stringMissing = string
      .filter(col("targetB").isNull && col("speciesB.taxon_id") === 9606)
      .select("intB")

    val unionUnmatch = intactMissing.unionByName(stringMissing)

    val unmatch = unionUnmatch
      .select("intB")
      .distinct

    unmatch
  }

  /** @param dataframe with targetA and/or targetB with null value.
    * @return a DataFrame
    */
  def removeNullTargetA(df: DataFrame)(implicit ss: SparkSession): DataFrame = {
    df.filter(col("targetA").isNotNull)
  }

  /** Homo_sapiens.GRCh38.chr.gtf.gz is a tsv file with the first 5 lines are comments
    * @param interactionsConfiguration ensembl protein file path
    * @return a DataFrame
    */
  def transformEnsemblProtein(df: DataFrame)(implicit ss: SparkSession): DataFrame = {

    df.filter(col("_c2") === "CDS")
      .withColumn("gene_id", regexp_extract(col("_c8"), "ENSG\\w{11}", 0))
      .withColumn("protein_id", regexp_extract(col("_c8"), "ENSP\\w{11}", 0))
      .select("gene_id", "protein_id")
  }

  def compute()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss = context.sparkSession
    import ss.implicits._
    import InteractionsHelpers._

    logger.info("Loading raw inputs for Interactin step.")
    val interactionsConfiguration = context.configuration.interactions

    val mappedInputs = Map(
      "targets" -> interactionsConfiguration.targetEtl,
      "rnacentral" -> interactionsConfiguration.rnacentral,
      "humanmapping" -> interactionsConfiguration.humanmapping,
      "intact" -> interactionsConfiguration.intact,
      "ensproteins" -> interactionsConfiguration.ensproteins,
      "strings" -> interactionsConfiguration.strings
    )

    val inputDataFrame = IoHelpers.readFrom(mappedInputs)
    // String dataset needs some transformation.
    val stringDataframe =
      StringProtein(inputDataFrame("strings").data, interactionsConfiguration.scorethreshold)

    val ensproteins = inputDataFrame("ensproteins").data.transform(transformEnsemblProtein)
    val interactionStringsDF = stringDataframe.generateStrings(ensproteins)

    val interactionIntactDF = inputDataFrame("intact").data.generateIntacts(
      inputDataFrame("targets").data,
      inputDataFrame("rnacentral").data,
      inputDataFrame("humanmapping").data
    )

    /** The filter is applied here in order to retrieve the unmatched interaction */
    val interactionIntactDFValid = removeNullTargetA(interactionIntactDF)
    val interactionStringsDFValid = removeNullTargetA(interactionStringsDF)

    val aggregationInteractions =
      interactionIntactDFValid.interactionAggreation(interactionStringsDFValid)
    val interactionEvidences = interactionIntactDFValid
      .generateEvidences(interactionStringsDFValid)
      .repartitionByRange(500, $"targetA".asc, $"targetB".asc)

    val outputs = context.configuration.interactions.outputs

    Map(
      "interactionsEvidence" -> IOResource(interactionEvidences, outputs.interactionsEvidence),
      "interactions" -> IOResource(aggregationInteractions, outputs.interactions),
      // This can be transformed into a ammonite script.
      "interactionUnmatch" -> IOResource(
        getUnmatch(interactionIntactDF, interactionStringsDF),
        outputs.interactionsUnmatched
      )
    )
  }

  def apply()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession

    val otnetworksDF = compute()
    IoHelpers.writeTo(otnetworksDF)
  }
}
