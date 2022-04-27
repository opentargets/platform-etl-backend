package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers
import io.opentargets.etl.backend.spark.Helpers._
import io.opentargets.etl.backend.target.TargetUtils.transformArrayToStruct
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{array, col, element_at, explode, split, typedLit}

case class Hgnc(
    ensemblId: String,
    hgncId: Array[IdAndSource],
    approvedSymbol: String,
    approvedName: String,
    hgncSynonyms: Option[Array[LabelAndSource]],
    hgncSymbolSynonyms: Option[Array[LabelAndSource]],
    hgncNameSynonyms: Option[Array[LabelAndSource]],
    hgncObsoleteSymbols: Option[Array[LabelAndSource]],
    hgncObsoleteNames: Option[Array[LabelAndSource]],
    uniprotIds: Option[Array[String]]
)
object Hgnc extends LazyLogging {

  def apply(hgncRaw: DataFrame)(implicit ss: SparkSession): Dataset[Hgnc] = {
    logger.info("Transforming HGNC inputs.")
    val hgnc = hgncRaw.select(explode(col("response.docs"))).select("col.*")
    selectAndRenameFields(hgnc)

  }

  private def selectAndRenameFields(
      dataFrame: DataFrame
  )(implicit ss: SparkSession): Dataset[Hgnc] = {
    import ss.implicits._
    val hgncDf = dataFrame
      .select(
        col("ensembl_gene_id") as "ensemblId",
        split(col("hgnc_id"), ":") as "hgncId",
        col("symbol") as "approvedSymbol",
        col("name") as "approvedName",
        col("uniprot_ids"),
        safeArrayUnion(
          col("prev_name"),
          col("prev_symbol"),
          col("alias_symbol"),
          col("alias_name")
        ) as "hgncSynonyms",
        safeArrayUnion(col("alias_symbol")) as "hgncSymbolSynonyms",
        safeArrayUnion(col("alias_name")) as "hgncNameSynonyms",
        safeArrayUnion(col("prev_symbol")) as "hgncObsoleteSymbols",
        safeArrayUnion(col("prev_name")) as "hgncObsoleteNames"
      )
      .transform(Helpers.snakeToLowerCamelSchema)

    val synonyms = List(
      "hgncSynonyms",
      "hgncSymbolSynonyms",
      "hgncNameSynonyms",
      "hgncObsoleteSymbols",
      "hgncObsoleteNames"
    )
      .foldLeft(hgncDf) { (B, name) =>
        B.withColumn(
          name,
          transformArrayToStruct(col(name), typedLit("HGNC") :: Nil, labelAndSourceSchema)
        )
      }

    val hgncWithDbRef = synonyms
      .withColumn("id", element_at(col("hgncId"), 2))
      .withColumn("source", element_at(col("hgncId"), 1))
      .drop("hgncId")
      .transform(nest(_, List("id", "source"), "hgncId"))
      .withColumn("hgncId", array(col("hgncId")))

    hgncWithDbRef
      .dropDuplicates("ensemblId")
      .as[Hgnc]
  }
}
