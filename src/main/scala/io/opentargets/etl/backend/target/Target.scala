package io.opentargets.etl.backend.target

import better.files.{File, InputStreamExtensions}
import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers.{nest, safeArrayUnion}
import io.opentargets.etl.backend.{Configuration, ETLSessionContext}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import io.opentargets.etl.backend.spark.{CsvHelpers, IOResource, IOResourceConfig, IoHelpers}
import io.opentargets.etl.preprocess.uniprot.UniprotConverter
import org.apache.spark.sql.functions.{
  array_union,
  coalesce,
  col,
  collect_set,
  explode,
  flatten,
  typedLit
}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

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
    val uniprotDf: Dataset[Uniprot] = Uniprot(inputDataFrames("uniprot").data)
    val geneOntologyDf: Dataset[GeneOntologyByEnsembl] = GeneOntology(
      inputDataFrames("geneOntologyHuman").data,
      inputDataFrames("geneOntologyRna").data,
      inputDataFrames("geneOntologyRnaLookup").data,
      ensemblDf)
    val tep: Dataset[TepWithId] = Tep(inputDataFrames("tep").data)

    // merge intermediate data frames into final
    val hgncEnsemblTepGO = mergeHgncAndEnsembl(hgnc, ensemblDf)
      .join(tep, ensemblDf("id") === tep("ensemblId"), "left_outer")
      .drop("ensemblId")
      .join(geneOntologyDf, ensemblDf("id") === geneOntologyDf("ensemblId"), "left_outer")
      .drop("ensemblId")

    val uniprotGroupedByEnsemblId = addEnsemblIdsToUniprot(hgnc, uniprotDf)
      .withColumnRenamed("proteinIds", "pid")

    // todo hngc: merge synonyms: - merge name and symbol synonyms
    // todo add hgnc to dbxrefs
    hgncEnsemblTepGO
      .join(uniprotGroupedByEnsemblId, Seq("id"), "left_outer")
      .withColumn("proteinIds", safeArrayUnion(col("proteinIds"), col("pid")))
      .withColumn("dbXrefs", safeArrayUnion(col("hgncId"), flatten(col("dbXrefs"))))
      .withColumn("synonyms", safeArrayUnion(flatten(col("synonyms")), col("hgncSynonyms")))
      .drop("pid", "hgncId", "hgncSynonyms", "uniprotIds")
  }

  def addEnsemblIdsToUniprot(hgnc: Dataset[Hgnc], uniprot: Dataset[Uniprot]): DataFrame = {
    logger.debug("Grouping Uniprot entries by Ensembl Id.")
    hgnc
      .select(col("ensemblId"), explode(col("uniprotIds")).as("uniprotId"))
      .withColumn("id", col("uniprotId"))
      .withColumn("source", typedLit("Uniprot"))
      .transform(nest(_, List("id", "source"), "uniprotProteinId"))
      .join(uniprot, Seq("uniprotId"))
      .groupBy("ensemblId")
      .agg(
        collect_set(col("synonyms")).as("synonyms"),
        collect_set(col("functionDescriptions")).as("functionDescriptions"),
        collect_set(col("proteinIds")).as("proteinIds"),
        collect_set(col("subcellularLocations")).as("subcellularLocations"),
        collect_set(col("dbXrefs")).as("dbXrefs"),
        collect_set(col("uniprotProteinId")).as("uniprotProteinId")
      )
      .withColumnRenamed("ensemblId", "id")
      .withColumn("proteinIds", safeArrayUnion(flatten(col("proteinIds")), col("uniprotProteinId")))
      .drop("uniprotId", "uniprotProteinId")
  }

  /** Return map on input IOResources */
  private def getMappedInputs(targetConfig: Configuration.Target)(
      implicit sparkSession: SparkSession): Map[String, IOResource] = {
    def getUniprotDataFrame(io: IOResourceConfig)(implicit ss: SparkSession): IOResource = {
      import ss.implicits._
      val file = io.path match {
        case f if f.endsWith("gz") => File(f).newInputStream.asGzipInputStream().lines
        case f_                    => File(f_).lineIterator
      }
      val data = UniprotConverter.convertUniprotFlatFileToUniprotEntry(file)
      IOResource(data.toDF(), io)
    }

    val mappedInputs = Map(
      "hgnc" -> IOResourceConfig(
        targetConfig.input.hgnc.format,
        targetConfig.input.hgnc.path
      ),
      "orthologs" -> IOResourceConfig(
        targetConfig.input.ortholog.format,
        targetConfig.input.ortholog.path,
        options = CsvHelpers.tsvWithHeader
      ),
      "ensembl" -> IOResourceConfig(
        targetConfig.input.ensembl.format,
        targetConfig.input.ensembl.path
      ),
      "geneOntologyHuman" -> IOResourceConfig(
        targetConfig.input.geneOntology.format,
        targetConfig.input.geneOntology.path,
        options = targetConfig.input.geneOntology.options
      ),
      "geneOntologyRna" -> IOResourceConfig(
        targetConfig.input.geneOntologyRna.format,
        targetConfig.input.geneOntologyRna.path,
        options = targetConfig.input.geneOntologyRna.options
      ),
      "geneOntologyRnaLookup" -> IOResourceConfig(
        targetConfig.input.geneOntologyRnaLookup.format,
        targetConfig.input.geneOntologyRnaLookup.path,
        options = targetConfig.input.geneOntologyRnaLookup.options
      ),
      "tep" -> IOResourceConfig(
        targetConfig.input.tep.format,
        targetConfig.input.tep.path
      )
    )

    IoHelpers
      .readFrom(mappedInputs)
      .updated("uniprot",
               getUniprotDataFrame(
                 IOResourceConfig(
                   targetConfig.input.uniprot.format,
                   targetConfig.input.uniprot.path
                 )))
  }

  /** Merge Hgnc and Ensembl datasets in a way that preserves logic of data pipeline.
    *
    * The deprecated data pipeline build up the target dataset in a step-wise manner, where later steps only added
    * fields if they were not already provided by an earlier one. This method reproduces that logic so that fields
    * provided on both datasets are set by Hgnc.
    *
    * @param hgnc
    * @param ensembl
    * @return
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
    // todo Update hgnc uniprotIds to use the IdAndSource case class
    // todo check what needs to happen to merge unused approvedNames...probably go to symbol synonyms.
    merged
  }

}
