package io.opentargets.etl.backend.cancerbiomarkers

import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import io.opentargets.etl.backend.spark.{IOResource, IOResourceConfig, IoHelpers}
import io.opentargets.etl.backend.{Configuration, ETLSessionContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class CancerSource(name: String, description: String, link: String)

case class CancerBiomarker(id: String,
                           drugName: String,
                           target: String,
                           disease: String,
                           evidenceLevel: String,
                           associationType: String,
                           sourcesPubmed: Array[Long],
                           sourcesOther: Array[CancerSource])

object CancerBiomarkers {
  def apply()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession

    val inputs = getMappedInputs(context.configuration.cancerbiomarkers)

    val cancerBiomarkersDS: Dataset[CancerBiomarker] = compute(inputs("biomarkers").data,
                                                               inputs("sourceMapping").data,
                                                               inputs("diseaseMapping").data,
                                                               inputs("targetMapping").data)

    val dataframesToSave: IOResources = Map(
      "cancerbiomarkers" -> IOResource(cancerBiomarkersDS.toDF,
                                       context.configuration.cancerbiomarkers.output)
    )
    IoHelpers.writeTo(dataframesToSave)
  }

  def compute(
      biomarkers: DataFrame,
      sourceDF: DataFrame,
      diseaseDF: DataFrame,
      targetDF: DataFrame)(implicit context: ETLSessionContext): Dataset[CancerBiomarker] = {
    implicit val ss: SparkSession = context.sparkSession
    import ss.implicits._

    val efoIds: DataFrame = diseaseDF
      .select(regexp_replace(col("name"), "_", "") as "tumor_type",
              regexp_extract(col("url"), "[^/]+$", 0) as "disease")
    // target step output used to map gene name to ensembl id.
    val ensemblIdLookup: DataFrame = targetDF
      .select(
        col("id") as "target",
        col("approvedSymbol") as "Gene"
      )

    val biomarkerPreprocess = biomarkers
      .select(
        coalesce(col("IndividualMutation"), col("Biomarker")) as "id",
        col("Association") as "associationType",
        col("DrugFullName") as "drugName",
        col("EvidenceLevel") as "evidenceLevel",
        split(col("Source"), ";") as "Source",
        split(col("Gene"), ";") as "Gene",
        split(translate(col("PrimaryTumorTypeFullName"), " -", ""), ";") as "tumor_type"
      )
      .withColumn("Gene", array_distinct(col("Gene")))
      .withColumn("tumor_type", explode(col("tumor_type")))

    // add in disease information
    val biomarkerWithDisease = biomarkerPreprocess
      .join(efoIds, Seq("tumor_type"), "left_outer")
      .drop("name")

    // override specific genes and turn GeneId into ENSG ID
    val biomarkerWithDiseaseAndGene = biomarkerWithDisease
      .withColumn("Gene", explode(col("Gene")))
      .withColumn("Gene",
                  when(col("Gene") === "C15orf55", "NUTM1")
                    .when(col("Gene") === "MLL", "KMT2a")
                    .when(col("Gene") === "MLL2", "KMT2D")
                    .otherwise(col("Gene")))
      .withColumn("Gene", upper(col("Gene"))) // call upper for
      .join(ensemblIdLookup, Seq("Gene"))
      .drop("Gene")
      .withColumn("uid", monotonically_increasing_id)

    // pubmed references
    val biomarkerWithDiseaseGenePubmedDF = biomarkerWithDiseaseAndGene
      .withColumn("Source", array_distinct(col("Source")))
      .withColumn("Source", explode(col("Source")))
      .withColumn("pmid",
                  when(col("Source").startsWith("PMID"),
                       expr("substring(Source, 6,length(Source))").cast(LongType)))
      .withColumn(
        "clinicalTrial",
        when(
          col("Source").startsWith("NCT"),
          struct(
            col("Source") as "name",
            lit("Clinical Trial") as "description",
            concat(lit("https://clinicaltrials.gov/ct2/show/"), col("Source")) as "link"
          )
        )
      )
      .groupBy({
        "clinicalTrial" +: biomarkerWithDiseaseAndGene.columns
      }.map(col): _*)
      .agg(collect_set("pmid") as "sourcesPubmed")

    val otherSourcesDF = biomarkerWithDiseaseGenePubmedDF
      .select(col("uid"), trim(regexp_extract(col("Source"), "[\\w ]+", 0)) as "source")
      .join(sourceDF, Seq("source"), "left_outer")
      .withColumn(
        "otherReferences",
        when(!(col("Source").startsWith("NCT") || col("Source").startsWith("PMID")),
             struct(
               col("source") as "name",
               col("label") as "description",
               col("url") as "link"
             ))
          .otherwise(null)
      )
      .drop("source", "label", "url")

    val flatten_distinct = array_distinct _ compose flatten

    val biomarkerDiseaseGeneSources = biomarkerWithDiseaseGenePubmedDF
      .join(otherSourcesDF, Seq("uid"), "left_outer")
      .withColumn("s", array("otherReferences", "clinicalTrial"))
      .distinct
      .groupBy("uid", "id", "associationType", "drugName", "evidenceLevel", "target", "disease")
      .agg(flatten_distinct(collect_set("sourcesPubmed")) as "sourcesPubmed",
           flatten_distinct(collect_set("s")) as "sourcesOther")
      .drop("uid")

    biomarkerDiseaseGeneSources.as[CancerBiomarker]
  }

  /** Return map on input IOResources */
  private def getMappedInputs(config: Configuration.CancerBiomarkersSection)(
      implicit sparkSession: SparkSession): Map[String, IOResource] = {

    val inputs = config.inputs

    val mappedInputs = Map(
      "biomarkers" -> IOResourceConfig(
        inputs.biomarkers.format,
        inputs.biomarkers.path,
        inputs.biomarkers.options
      ),
      "diseaseMapping" -> IOResourceConfig(
        inputs.diseaseMapping.format,
        inputs.diseaseMapping.path
      ),
      "sourceMapping" -> IOResourceConfig(
        inputs.diseaseMapping.format,
        inputs.sourceMapping.path
      ),
      "targetMapping" -> IOResourceConfig(
        inputs.targetMapping.format,
        inputs.targetMapping.path
      )
    )
    IoHelpers
      .readFrom(mappedInputs)
  }
}
