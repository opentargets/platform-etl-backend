package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, explode}

case class Hgnc(ensemblId: String,
                hgncId: String,
                approvedSymbol: String,
                approvedName: String,
                status: String,
                locusGroup: String,
                symbolSynonyms: Array[String],
                nameSynonyms: Array[String],
                enzymeId: Array[String],
                entrezId: String,
                uniprotIds: Array[String],
                pubmedIds: Array[Long])
object Hgnc extends LazyLogging {

  def apply(hgncRaw: DataFrame)(implicit ss: SparkSession): Dataset[Hgnc] = {
    logger.info("Transforming HGNC inputs.")
    val hgnc = hgncRaw.select(explode(col("response.docs"))).select("col.*")
    selectAndRenameFields(hgnc)

  }

  private def selectAndRenameFields(dataFrame: DataFrame)(implicit ss: SparkSession): Dataset[Hgnc] = {
    import ss.implicits._
    val hgncDf = dataFrame.select(
      col("ensembl_gene_id") as "ensemblId",
      col("hgnc_id"),
      col("symbol") as "approvedSymbol",
      col("name") as "approvedName",
      col("status"),
      col("locus_group"),
      col("alias_symbol") as "symbolSynonyms",
      col("alias_name") as "nameSynonyms",
      col("enzyme_id"),
      col("entrez_id"),
      col("uniprot_ids"),
      col("pubmed_id") as "pubmedIds"
    ).transform(Helpers.snakeToLowerCamelSchema)

    hgncDf.as[Hgnc]
  }

}
