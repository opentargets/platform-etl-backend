import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

val ss: SparkSession = ???

/**
  * Final output in original:
root
 |-- alt_allele: string (nullable = true)
 |-- chr_id: string (nullable = true)
 |-- feature: string (nullable = true)
 |-- fpred_labels: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- fpred_max_label: string (nullable = true)
 |-- fpred_max_score: double (nullable = true)
 |-- fpred_scores: array (nullable = true)
 |    |-- element: double (containsNull = true)
 |-- gene_id: string (nullable = true)
 |-- position: long (nullable = true)
 |-- ref_allele: string (nullable = true)
 |-- source_id: string (nullable = true)
 |-- type_id: string (nullable = true)

  */

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
"sense_overlapping")

val targetRawDf: DataFrame = ss.read.parquet("/home/jarrod/development/platform-etl-backend/data/output/targets/*.parquet")
val variantRawDf: DataFrame = ss.read
  .parquet(
   "/home/jarrod/development/platform-etl-backend/data/output/variant-index/*.parquet")

val target = Gene.getGeneDf(targetRawDf, approved)

val idxCols = Seq("chr_id", "position", "ref_allele", "alt_allele").map(col)

val tssDistance = 500000
val nearests = variantRawDf
  .select(Seq(
   lit("distance") as "type_id",
   lit("canonical_tss") as "source_id",
   lit("unspecified") as "feature",
  ) ++ idxCols: _*)

val dists = target.transform(Gene.variantGeneDistance(nearests, tssDistance))
  .withColumn("distance_score", when(col("d") > 0, lit(1.0) / col("d")).otherwise(1.0))

val w = Window.partitionBy("source_id", "feature").orderBy(col("distance_score").asc)

val df = dists
  .withColumn("distance_score_q", round(percent_rank().over(w), 1))
  .select(idxCols ++ Seq("gene_id", "d", "distance_score").map(col):_*)
