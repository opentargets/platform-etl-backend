import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

val ss: SparkSession = ???
val approved = List(
  "3prime_overlapping_ncRNA",
  "antisense",
  "bidirectional_promoter_lncRNA",
  "IG_C_gene",
  "IG_D_gene",
  "IG_J_gene",
  "IG_V_gene",
  "lincRNA",
  "macro_lncRNA",
  "non_coding",
  "protein_coding",
  "sense_intronic",
  "sense_overlapping"
)

object Gene {

  def getGeneDf(dataframe: DataFrame, approvedBioTypes: List[String]): DataFrame = {
    val excludedChromosomes: Set[String] = Set("MT")
    dataframe
      .select(
        col("id") as "gene_id",
        col("genomicLocation.*"),
        col("biotype"),
        when(col("genomicLocation.strand") > 0, col("genomicLocation.start"))
          .otherwise(col("genomicLocation.end")) as "tss"
      )
      .filter(
        (col("biotype") isInCollection approvedBioTypes.toSet) && !(col(
          "chromosome"
        ) isInCollection excludedChromosomes)
      )
  }

  def variantGeneDistance(variant: DataFrame, distance: Long)(target: DataFrame): DataFrame =
    variant
      .join(
        target,
        (col("chr_id") === col("chromosome")) && (abs(
          col("position") - col("tss")
        ) <= distance)
      )
      .withColumn("d", abs(col("position") - col("tss")))
}

val idxStr = Seq("chr_id", "position", "ref_allele", "alt_allele")
val idxCols = idxStr.map(col)

val targetRawDf: DataFrame =
  ss.read.parquet("/home/jarrod/development/platform-etl-backend/data/output/targets/*.parquet")
val variantRawDf: DataFrame = ss.read
  .parquet("/home/jarrod/development/platform-etl-backend/data/output/variant-index/*.parquet")

val qtlRawDf =
  ss.read.parquet("/home/jarrod/development/platform-etl-backend/data/genetics/vg/qtl/*.parquet")

val qtl = qtlRawDf.select(
  col("chrom") as "chr_id",
  col("pos") as "position",
  col("other_allele") as "ref_allele",
  col("effect_allele") as "alt_allele",
  col("beta") as "qtl_beta",
  col("se") as "qtl_se",
  when(col("pval") === 0d, lit(Double.MinPositiveValue)).otherwise(col("pval")) as "qtl_pval",
  col("ensembl_id") as "gene_id",
  col("type") as "type_id",
  col("source") as "source_id",
  col("feature")
).withColumn("qtl_score", -log(10, col("qtl_pval")))
  .join(variantRawDf, idxStr, "left_semi")

val w = Window.partitionBy("source_id", "feature").orderBy(col("qtl_score").asc)

val df = qtl.withColumn("qtl_score_q", round(percent_rank().over(w), 1))