package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers
import io.opentargets.etl.backend.spark.Helpers.{nest, safeArrayUnion}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{array, col, element_at, explode, split}

case class Hgnc(ensemblId: String,
                hgncId: Array[IdAndSource],
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
        split(col("hgnc_id"), ":") as "hgncId",
        col("symbol") as "approvedSymbol",
        col("name") as "approvedName",
        col("uniprot_ids"),
        safeArrayUnion(col("prev_symbol"), col("alias_symbol"), col("alias_name")) as "hgncSynonyms"
      )
      .transform(Helpers.snakeToLowerCamelSchema)

    val synonyms =
      TargetUtils.transformColumnToLabelAndSourceStruct(hgncDf, "ensemblId", "hgncSynonyms", "HGNC")
    val hgncWithDbRef = hgncDf
      .withColumn("id", element_at(col("hgncId"), 2))
      .withColumn("source", element_at(col("hgncId"), 1))
      .drop("hgncId")
      .transform(nest(_, List("id", "source"), "hgncId"))
      .withColumn("hgncId", array(col("hgncId")))

    hgncWithDbRef
      .drop("hgncSynonyms")
      .join(synonyms, Seq("ensemblId"), "left_outer")
      .dropDuplicates("ensemblId")
      .as[Hgnc]
  }

}
