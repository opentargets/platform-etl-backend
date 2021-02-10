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

    val ensembl: Option[Dataset[Ensembl]] =
      inputDataFrame.get("ensembl").map(ioResource => Ensembl(ioResource.data))

    val uniprot: Option[Dataset[Uniprot]] =
      inputDataFrame.get("uniprot").map(ioResource => Uniprot(ioResource.data))

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
