package io.opentargets.etl.backend.target

import better.files.{File, InputStreamExtensions}
import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import io.opentargets.etl.backend.spark.{CsvHelpers, IOResource, IOResourceConfig, IoHelpers}
import io.opentargets.etl.preprocess.uniprot.UniprotConverter
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Target extends LazyLogging {
  def apply()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession

    val targetDF = compute(context)

    ???
  }

  def compute(context: ETLSessionContext)(implicit ss: SparkSession): DataFrame = {

    val targetConfig = context.configuration.target
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

    val inputDataFrame = IoHelpers
      .readFrom(mappedInputs)
      .updated("uniprot",
               getUniprotDataFrame(
                 IOResourceConfig(
                   targetConfig.input.uniprot.format,
                   targetConfig.input.ensembl.path
                 )))

    val hgnc: Option[Dataset[Hgnc]] =
      inputDataFrame.get("hgnc").map(ioResource => Hgnc(ioResource.data))

    //    val ortholog: Option[DataFrame] =
    //      inputDataFrame
    //        .get("orthologs")
    //        .map(ioResource => Ortholog(ioResource.data, targetConfig.hgncOrthologSpecies))

    val ensemblDf: Option[Dataset[Ensembl]] =
      inputDataFrame.get("ensembl").map(ioResource => Ensembl(ioResource.data))

    val uniprotDf: Option[Dataset[Uniprot]] =
      inputDataFrame.get("uniprot").map(ioResource => Uniprot(ioResource.data))

    val geneOntologyDf: Option[Dataset[GeneOntologyByEnsembl]] = List(
      inputDataFrame.get("geneOntologyHuman"),
      inputDataFrame.get("geneOntologyRna"),
      inputDataFrame.get("geneOntologyRnaLookup")).flatten match {
      case human :: rna :: ids :: Nil =>
        Some(GeneOntology(human.data, rna.data, ids.data, ensemblDf.get))
      case _ =>
        logger.warn(s"One or more inputs was missing for Gene Ontology substep of Target.")
        None
    }

    val tep: Option[Dataset[TepWithId]] =
      inputDataFrame.get("tep").map(ioResource => Tep(ioResource.data))

    ???
  }

  private def getUniprotDataFrame(io: IOResourceConfig)(implicit ss: SparkSession): IOResource = {
    import ss.implicits._
    val file = io.path match {
      case f if f.endsWith("gz") => File(f).newInputStream.asGzipInputStream().lines
      case f_                    => File(f_).lineIterator
    }
    val data = UniprotConverter.convertUniprotFlatFileToUniprotEntry(file)
    IOResource(data.toDF(), io)
  }

}
