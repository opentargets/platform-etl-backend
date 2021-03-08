package io.opentargets.etl.backend.target

import better.files.{File, InputStreamExtensions}
import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers.{mkFlattenArray, nest, safeArrayUnion}
import io.opentargets.etl.backend.{Configuration, ETLSessionContext}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import io.opentargets.etl.backend.spark.{CsvHelpers, IOResource, IOResourceConfig, IoHelpers}
import io.opentargets.etl.preprocess.uniprot.UniprotConverter
import org.apache.spark.sql.functions.{
  array,
  array_union,
  coalesce,
  col,
  collect_set,
  explode,
  flatten,
  trim,
  typedLit
}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.jdk.CollectionConverters.asScalaIteratorConverter

object Target extends LazyLogging {
  def apply()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession

    val targetDF = compute(context)

    val dataframesToSave: IOResources = Map(
      "target" -> IOResource(targetDF, context.configuration.target.outputs.target)
    )

    IoHelpers.writeTo(dataframesToSave)

  }

  def compute(context: ETLSessionContext)(implicit ss: SparkSession): DataFrame = {

    // get input data frames
    val inputDataFrames = getMappedInputs(context.configuration.target)

    // prepare intermediate dataframes per source
    val hgnc: Dataset[Hgnc] = Hgnc(inputDataFrames("hgnc").data)
    val ensemblDf: Dataset[Ensembl] = Ensembl(inputDataFrames("ensembl").data)
    val uniprotDS: Dataset[Uniprot] = Uniprot(inputDataFrames("uniprot").data)
    val geneOntologyDf: Dataset[GeneOntologyByEnsembl] = GeneOntology(
      inputDataFrames("geneOntologyHuman").data,
      inputDataFrames("geneOntologyRna").data,
      inputDataFrames("geneOntologyRnaLookup").data,
      ensemblDf)
    val tep: Dataset[TepWithId] = Tep(inputDataFrames("tep").data)
    val hpa: Dataset[GeneWithLocation] = GeneWithLocation(inputDataFrames("hpa").data)
    val projectScoresDS: Dataset[GeneWithDbXRef] = ProjectScores(
      inputDataFrames("projectScoresIds").data,
      inputDataFrames("projectScoresEssentialityMatrix").data)
    val proteinClassification: Dataset[ProteinClassification] = ProteinClassification(
      inputDataFrames("chembl").data)

    // merge intermediate data frames into final
    val hgncEnsemblTepGoDF = mergeHgncAndEnsembl(hgnc, ensemblDf)
      .join(tep, ensemblDf("id") === tep("ensemblId"), "left_outer")
      .drop("ensemblId")
      .join(geneOntologyDf, ensemblDf("id") === geneOntologyDf("ensemblId"), "left_outer")
      .drop("ensemblId")
      .join(projectScoresDS, Seq("id"), "left_outer")

    val uniprotGroupedByEnsemblIdDF =
      addEnsemblIdsToUniprot(hgnc,
                             addProteinClassificationToUniprot(uniprotDS, proteinClassification))
        .withColumnRenamed("proteinIds", "pid")
        .join(hpa, Seq("id"), "left_outer")
        .withColumn("subcellularLocations",
                    mkFlattenArray(col("subcellularLocations"), col("locations")))
        .drop("locations")

    hgncEnsemblTepGoDF
      .join(uniprotGroupedByEnsemblIdDF, Seq("id"), "left_outer")
      .withColumn("proteinIds", safeArrayUnion(col("proteinIds"), col("pid")))
      .withColumn("dbXrefs",
                  safeArrayUnion(col("hgncId"), col("dbXrefs"), col("signalP"), col("xRef")))
      .withColumn("synonyms", safeArrayUnion(col("synonyms"), col("hgncSynonyms")))
      .drop("pid", "hgncId", "hgncSynonyms", "uniprotIds", "signalP", "xRef")
  }

  def addEnsemblIdsToUniprot(hgnc: Dataset[Hgnc], uniprot: DataFrame): DataFrame = {
    logger.debug("Grouping Uniprot entries by Ensembl Id.")
    hgnc
      .select(col("ensemblId"), explode(col("uniprotIds")).as("uniprotId"))
      .withColumn("id", col("uniprotId"))
      .withColumn("source", typedLit("Uniprot"))
      .transform(nest(_, List("id", "source"), "uniprotProteinId"))
      .join(uniprot, Seq("uniprotId"))
      .groupBy("ensemblId")
      .agg(
        flatten(collect_set(col("synonyms"))).as("synonyms"),
        flatten(collect_set(col("functionDescriptions"))).as("functionDescriptions"),
        flatten(collect_set(col("proteinIds"))).as("proteinIds"),
        flatten(collect_set(col("subcellularLocations"))).as("subcellularLocations"),
        flatten(collect_set(col("dbXrefs"))).as("dbXrefs"),
        collect_set(col("uniprotProteinId")).as("uniprotProteinId"),
        flatten(collect_set(col("targetClass"))).as("targetClass")
      )
      .withColumnRenamed("ensemblId", "id")
      .withColumn("proteinIds", safeArrayUnion(col("proteinIds"), col("uniprotProteinId")))
      .drop("uniprotId", "uniprotProteinId")
  }

  /** Return map on input IOResources */
  private def getMappedInputs(targetConfig: Configuration.Target)(
      implicit sparkSession: SparkSession): Map[String, IOResource] = {
    def getUniprotDataFrame(io: IOResourceConfig)(implicit ss: SparkSession): IOResource = {
      import ss.implicits._
      val path: java.util.Iterator[String] = sparkSession.read.textFile(io.path).toLocalIterator()
      val data = UniprotConverter.convertUniprotFlatFileToUniprotEntry(path.asScala)
      IOResource(data.toDF(), io)
    }

    val targetInputs = targetConfig.input
    val mappedInputs = Map(
      "hgnc" -> IOResourceConfig(
        targetInputs.hgnc.format,
        targetInputs.hgnc.path
      ),
      "orthologs" -> IOResourceConfig(
        targetInputs.ortholog.format,
        targetInputs.ortholog.path,
        options = CsvHelpers.tsvWithHeader
      ),
      "ensembl" -> IOResourceConfig(
        targetInputs.ensembl.format,
        targetInputs.ensembl.path
      ),
      "geneOntologyHuman" -> IOResourceConfig(
        targetInputs.geneOntology.format,
        targetInputs.geneOntology.path,
        options = targetInputs.geneOntology.options
      ),
      "geneOntologyRna" -> IOResourceConfig(
        targetInputs.geneOntologyRna.format,
        targetInputs.geneOntologyRna.path,
        options = targetInputs.geneOntologyRna.options
      ),
      "geneOntologyRnaLookup" -> IOResourceConfig(
        targetInputs.geneOntologyRnaLookup.format,
        targetInputs.geneOntologyRnaLookup.path,
        options = targetInputs.geneOntologyRnaLookup.options
      ),
      "tep" -> IOResourceConfig(
        targetInputs.tep.format,
        targetInputs.tep.path
      ),
      "hpa" -> IOResourceConfig(
        targetInputs.hpa.format,
        targetInputs.hpa.path,
        options = targetInputs.hpa.options
      ),
      "projectScoresIds" -> IOResourceConfig(
        targetInputs.psGeneIdentifier.format,
        targetInputs.psGeneIdentifier.path,
        options = targetInputs.psGeneIdentifier.options
      ),
      "projectScoresEssentialityMatrix" -> IOResourceConfig(
        targetInputs.psEssentialityMatrix.format,
        targetInputs.psEssentialityMatrix.path,
        options = targetInputs.psEssentialityMatrix.options
      ),
      "chembl" -> IOResourceConfig(
        targetInputs.chembl.format,
        targetInputs.chembl.path
      )
    )

    IoHelpers
      .readFrom(mappedInputs)
      .updated("uniprot",
               getUniprotDataFrame(
                 IOResourceConfig(
                   targetInputs.uniprot.format,
                   targetInputs.uniprot.path
                 )))
  }

  private def addProteinClassificationToUniprot(
      uniprot: Dataset[Uniprot],
      proteinClassification: Dataset[ProteinClassification]): DataFrame = {
    logger.debug("Add protein classifications to UniprotDS")
    val proteinClassificationWithUniprot = uniprot
      .select(col("uniprotId"), col("proteinIds.id").as("pid"))
      .withColumn("uid", array(col("uniprotId")))
      .withColumn("pid", array_union(col("uid"), col("pid")))
      .select(col("uniprotId"), explode(col("pid")).as("pid"))
      .withColumn("pid", trim(col("pid")))
      .join(proteinClassification, col("pid") === proteinClassification("accession"), "left_outer")
      .drop("accession")
      .groupBy("uniprotId")
      .agg(flatten(collect_set(col("targetClass"))).as("targetClass"))

    uniprot.join(proteinClassificationWithUniprot, Seq("uniprotId"), "left_outer")
  }

  /** Merge Hgnc and Ensembl datasets in a way that preserves logic of data pipeline.
    *
    * The deprecated data pipeline build up the target dataset in a step-wise manner, where later steps only added
    * fields if they were not already provided by an earlier one. This method reproduces that logic so that fields
    * provided on both datasets are set by Hgnc.
    */
  private def mergeHgncAndEnsembl(hgnc: Dataset[Hgnc], ensembl: Dataset[Ensembl]): DataFrame = {
    logger.debug("Merging Hgnc and Ensembl datasets")
    val eDf = ensembl
      .withColumnRenamed("approvedName", "an")
      .withColumnRenamed("approvedSymbol", "as")

    val merged = eDf
    // this removes non-reference ensembl genes introduced by HGNC.
      .join(hgnc, eDf("id") === hgnc("ensemblId"))
      // if approvedName and approvedSymbol provided by HGNC use those, otherwise Ensembl.
      .withColumn("approvedName", coalesce(col("approvedName"), col("an"), typedLit("")))
      .withColumn("approvedSymbol", coalesce(col("approvedSymbol"), col("as"), typedLit("")))
      .drop("an", "as")
    logger.debug(
      s"Merged HGNC and Ensembl dataframe has columns: ${merged.columns.mkString("Array(", ", ", ")")}")
    // todo check what needs to happen to merge unused approvedNames...probably go to symbol synonyms.
    merged
  }

}