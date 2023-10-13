package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import org.apache.spark.sql.functions.{array, coalesce, col, collect_set, concat, length, lit, size, struct, sum, trim, when}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object Epmc extends LazyLogging {

  def apply()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession

    logger.info("Prepare EPMC evidence")

    val conf = context.configuration.epmc

    val inputDataFrames = IoHelpers.readFrom(
      Map(
        "cooccurences" -> conf.input.cooccurences
      )
    )

    val inputDf = inputDataFrames("cooccurences").data

    val cooccurencesDf = compute(
      inputDf,
      conf.excludedTargetTerms,
      conf.sectionsOfInterest
    )

    val evidence = cooccurencesDf
      .select(
        lit("europepmc") as "datasourceId",
        lit("literature") as "datatypeId",
        col("targetFromSourceId"),
        col("diseaseFromSourceMappedId"),
        col("resourceScore"),
        col("literature"),
        col("textMiningSentences"),
        col("pmcIds"),
        col("year") as "publicationYear"
      )
      .cache()

    val epmcCooccurrencesDf = epmcCooccurrences(inputDf)

    logger.info("EPMC disease target evidence saved.")
    if (conf.printMetrics) {
      logger.info(s"Number of evidence: ${cooccurencesDf.count()}")
      logger.info(
        s"Number of publications: ${cooccurencesDf.select(col("publicationIdentifier")).count()}"
      )
      logger.info(
        s"Number of publications without pubmed ID: ${cooccurencesDf
            .filter(col("publicationIdentifier").contains("PMC"))
            .select("publicationIdentifier")
            .distinct
            .count()}"
      )
      logger.info(
        s"Number of targets: ${evidence.select(col("targetFromSourceId")).distinct.count()}"
      )
      logger.info(
        s"Number of diseases: ${evidence.select(col("diseaseFromSourceMappedId")).distinct.count()}"
      )
      logger.info(
        f"Number of associations: ${evidence.select(col("diseaseFromSourceMappedId"), col("targetFromSourceId")).dropDuplicates().count()}"
      )
    }

    logger.info(s"Write EMPC data to ${conf.outputs.output.path}")
    val dataframesToSave = Map(
      // coalesce to maintain logic previously used by datateam. A single file is used for metrics calculations.
      "epmc" -> IOResource(evidence.coalesce(1), conf.outputs.output),
      "epmcCooccurrences" -> IOResource(epmcCooccurrencesDf, conf.outputs.epmcCooccurrences)
    )

    IoHelpers.writeTo(dataframesToSave)
  }

  def generateUri(keywordId: Column): Column =
    when(keywordId.startsWith("ENSG"),
      concat(lit("https://www.ensembl.org/Homo_sapiens/Gene/Summary?db=core;g="), keywordId))
      .when(keywordId.startsWith("CHEMBL"),
      concat(lit("https://www.ebi.ac.uk/chembl/compound_report_card/"), keywordId))
      .otherwise(
      concat(lit("https://www.ebi.ac.uk/ols/ontologies/efo/terms?short_form="), keywordId))


  private def mapCoocurrenceType(cType: Column): Column =
    when(cType === "DS-CD",
      lit("Disease Drug Relationship"))
      .when(cType === "GP-CD",
        lit("Gene Drug Relationship"))
      .when(cType === "GP-DS",
        lit("Gene Disease Relationship"))

  private def epmcCooccurrences(cooccurences: DataFrame): DataFrame = {
    cooccurences
      .select(
        when(col("pmcid").isNotNull,
          lit("PMC"))
          .otherwise(lit("MED"))
          .as("src"),
        when(col("pmcid").isNotNull,
          col("pmcid"))
          .otherwise(col("pmid"))
          .as("id"),
        mapCoocurrenceType(col("type")).as("type"),
        col("text").as("exact"),
        col("section").as("section"),
        array(
          struct(
            col("label1").as("name"),
            generateUri(col("keywordId1")).as("uri")),
          struct(
            col("label2").as("name"),
            generateUri(col("keywordId2")).as("uri")),
        ).as("tags")
      )
      .groupBy("src", "id")
      .agg(
        collect_set(
          struct(
            col("type"),
            col("exact"),
            col("section"),
            col("tags")
          )
        ).as("anns")
      )
      .withColumn("provider", lit("OpenTargets"))
      .repartition(1)
      .persist()
  }

  private def compute(
      df: DataFrame,
      excludedTerms: List[String],
      sectionOfInterest: List[String]
  ): DataFrame =
    df.filter(col("section").isin(sectionOfInterest: _*))
      .withColumn("pmid", trim(col("pmid") cast StringType))
      .withColumn("publicationIdentifier", coalesce(col("pmid"), col("pmcid")))
      .filter(
        col("type") === "GP-DS" &&
          col("isMapped") &&
          col("publicationIdentifier").isNotNull &&
          length(col("text")) < 600 && !col("label1").isin(excludedTerms: _*)
      )
      .withColumnRenamed("keywordId1", "targetFromSourceId")
      .withColumnRenamed("keywordId2", "diseaseFromSourceMappedId")
      .groupBy("publicationIdentifier", "targetFromSourceId", "diseaseFromSourceMappedId", "year")
      .agg(
        collect_set(col("pmcid")).alias("pmcIds"),
        collect_set(col("pmid")).alias("literature"),
        collect_set(
          struct(
            col("text"),
            col("start1").alias("tStart"),
            col("end1").alias("tEnd"),
            col("start2").alias("dStart"),
            col("end2").alias("dEnd"),
            col("section")
          )
        ).alias("textMiningSentences"),
        sum(col("evidence_score")).alias("resourceScore")
      )
      .withColumn("pmcIds", when(size(col("pmcIds")) =!= 0, col("pmcIds")))
      .filter(col("resourceScore") > 1)
      .cache()

}
