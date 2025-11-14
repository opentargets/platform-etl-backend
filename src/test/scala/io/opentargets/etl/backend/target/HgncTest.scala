package io.opentargets.etl.backend.target

import io.opentargets.etl.backend.Configuration.{OTConfig, TargetSection}
import io.opentargets.etl.backend.spark.IoHelpers.IOResourceConfigurations
import io.opentargets.etl.backend.{Configuration, EtlSparkUnitTest}
import io.opentargets.etl.backend.target.HgncTest.hgncRawDf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import pureconfig.ConfigReader

object HgncTest {
  def hgncRawDf(implicit sparkSession: SparkSession): DataFrame =
    sparkSession.read
      .option("sep", "\t")
      .option("header", "true")
      .option("nullValue", "null")
      .csv(this.getClass.getResource("/target/hgnc_test.txt").getPath)
}

class HgncTest extends EtlSparkUnitTest {

  val selectAndRenameFields: PrivateMethod[Dataset[Hgnc]] =
    PrivateMethod[Dataset[Hgnc]]('selectAndRenameFields)

  val prepareInputDataFrame: PrivateMethod[Dataset[Hgnc]] =
    PrivateMethod[Dataset[Hgnc]]('prepareInputDataFrame)

  "HGNC" should "convert raw dataframe into HGCN objects without loss" in {
    val targetSection = TargetSection(
      null.asInstanceOf[IOResourceConfigurations],
      output = null.asInstanceOf[IOResourceConfigurations],
      hgncArrayColumns = List(
        "alias_name",
        "ccds_id",
        "ena",
        "enzyme_id",
        "gene_group",
        "gene_group_id",
        "lsdb",
        "mane_select",
        "mgd_id",
        "omim_id",
        "prev_name",
        "prev_symbol",
        "alias_symbol",
        "pubmed_id",
        "refseq_accession",
        "rgd_id",
        "rna_central_id",
        "uniprot_ids"
      ),
      hgncOrthologSpecies = List()
    )
    // given
    val df = Hgnc invokePrivate prepareInputDataFrame(hgncRawDf, targetSection)
    // when
    val results = Hgnc invokePrivate selectAndRenameFields(df, sparkSession)
    // then
    results.count must equal(hgncRawDf.select(col("ensembl_gene_id")).distinct.count)
  }
}
