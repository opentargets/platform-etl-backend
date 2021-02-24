package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers
import io.opentargets.etl.backend.spark.Helpers.safeArrayUnion
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, explode}

case class Hgnc(ensemblId: String,
                hgncId: String,
                approvedSymbol: String,
                approvedName: String,
                hgncSynonyms: Array[LabelAndSource],
                uniprotIds: Array[String],
)
object Hgnc extends LazyLogging {

  def apply(hgncRaw: DataFrame)(implicit ss: SparkSession): Dataset[Hgnc] = {
    logger.info("Transforming HGNC inputs.")
    val hgnc = hgncRaw.select(explode(col("response.docs"))).select("col.*")
    selectAndRenameFields(hgnc)

  }

  private def selectAndRenameFields(dataFrame: DataFrame)(
      implicit ss: SparkSession): Dataset[Hgnc] = {
    import ss.implicits._
    val hgncDf = dataFrame
      .select(
        col("ensembl_gene_id") as "ensemblId",
        col("hgnc_id"),
        col("symbol") as "approvedSymbol",
        col("name") as "approvedName",
        col("uniprot_ids"),
        safeArrayUnion(col("alias_symbol"), col("alias_name")) as "hgncSynonyms"
      )
      .transform(Helpers.snakeToLowerCamelSchema)

    val synonyms =
      TargetUtils.transformColumnToLabelAndSourceStruct(hgncDf, "ensemblId", "hgncSynonyms", "HGNC")

    hgncDf.drop("hgncSynonyms").join(synonyms, Seq("ensemblId"), "left_outer").as[Hgnc]
  }

}
