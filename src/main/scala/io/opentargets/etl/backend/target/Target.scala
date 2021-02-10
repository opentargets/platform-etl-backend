package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import io.opentargets.etl.backend.spark.{CsvHelpers, IOResourceConfig, IoHelpers}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Target extends LazyLogging {
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
      "uniprot" -> IOResourceConfig(
        targetConfig.input.uniprot.format,
        targetConfig.input.uniprot.path,
        options = targetConfig.input.uniprot.options
      )
    )

    val inputDataFrame = IoHelpers.readFrom(mappedInputs)

    val hgnc: Option[Dataset[Hgnc]] =
      inputDataFrame.get("hgnc").map(iOResource => Hgnc(iOResource.data))

    val ortholog: Option[DataFrame] =
      inputDataFrame
        .get("orthologs")
        .map(ioResource => Ortholog(ioResource.data, targetConfig.hgncOrthologSpecies))

    val ensembl: Option[Dataset[Ensembl]] =
      inputDataFrame.get("ensembl").map(ioResource => Ensembl(ioResource.data))

    val uniprot = inputDataFrame.get("uniprot").map(ioResource => Uniprot(ioResource.data))

    ???
  }

  def apply()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession

    val targetDF = compute(context)

    ???
  }
}
