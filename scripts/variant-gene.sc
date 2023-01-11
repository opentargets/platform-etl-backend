import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}


object Gene {

  def getGeneDf(dataframe: DataFrame): DataFrame = {
    dataframe
      .select(
        col("id") as "gene_id",
        col("genomicLocation.*"),
        col("biotype"),
        when(col("genomicLocation.strand") > 0, col("genomicLocation.start"))
          .otherwise(col("genomicLocation.end")) as "tss"
      )
  }

  /** @param variant  genetic variant
    * @param distance absolute difference between variant location and gene transcription start site.
    * @param target   index returned by Gene.getGeneDf
    * @return variants combined with targets on the same chromosome and within the prescibed distance.
    */
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


def unionDataframeDifferentSchema(df: DataFrame, df2: DataFrame): DataFrame = {
  val cols1 = df.columns.toSet
  val cols2 = df2.columns.toSet
  val total = cols1 ++ cols2 // union

  // Union between two dataframes with different schema. columnExpr helps to unify the schema
  val unionDF =
    df.select(columnExpr(cols1, total).toList: _*)
      .unionByName(df2.select(columnExpr(cols2, total).toList: _*))
  unionDF
}

def unionDataframeDifferentSchema(df: Seq[DataFrame]): DataFrame =
  df.reduce((a, b) => unionDataframeDifferentSchema(a, b))

def columnExpr(myCols: Set[String], allCols: Set[String]): Set[Column] = {
  val inter = (allCols intersect myCols).map(col)
  val differ = (allCols diff myCols).map(lit(null).as(_))

  inter union differ
}


val ss: SparkSession = ???
import ss.implicits._

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


val csqs: String => DataFrame = (path: String) => ss.read
  .format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .option("sep", "\\t")
  .option("ignoreLeadingWhiteSpace", "true")
  .option("ignoreTrailingWhiteSpace", "true")
  .option("mode", "DROPMALFORMED")
  .load(path)
  .filter(col("v2g_score").isNotNull)
  .select(
    regexp_extract(col("Accession"), ".+/(.+)", 1) as "accession",
    col("Term") as "term",
    col("Description") as "description",
    col("Display term") as "display_term",
    col("IMPACT") as "impact",
    col("v2g_score"),
    col("eco_score"),
  )


val idxStrs: Seq[String] = Seq("chr_id", "position", "ref_allele", "alt_allele")
val idxCols: Seq[Column] = idxStrs.map(col)

def calculateVep(variants: DataFrame, vep: DataFrame): DataFrame = {
  val groupingCols = idxCols :+ col("vep_gene_id")

  val variantIdx: DataFrame = variants
    .select(
      col("chr_id"),
      col("position"),
      col("ref_allele"),
      col("alt_allele"),
      explode(col("vep")) as "vep"
    )
    .select(
      idxCols :+ col("vep.*"): _*
    )

  // veps
  val vepConsequencesDf = vep
    .filter(col("v2g_score").isNotNull)
    .select(
      col("Term") as "fpred_label",
      col("v2g_score") as "fpred_score"
    )

  val variantGeneVepDf = variantIdx
    .select(groupingCols :+ (explode(col("fpred_labels")) as "fpred_label"): _*)
    .join(vepConsequencesDf, Seq("fpred_label"), "left_outer")
    .groupBy(groupingCols: _*)
    .agg(collect_set(struct(col("fpred_score"), col("fpred_label"))) as "f")
    .select(
      idxCols ++ Seq(
        map_filter(map_from_entries(col("f")), (k, _) => k > 0f).as("score_label_map"),
        col("f.fpred_score") as "fpred_scores",
        col("f.fpred_label") as "fpred_labels",
        lit("fpred").as("type_id"),
        lit("vep").as("source_id"),
        lit("unspecified").as("feature")
      ): _*
    )
    .withColumn("fpred_max_score", array_max(map_keys(col("score_label_map"))))
    .withColumn("fpred_max_label", element_at(col("score_label_map"), col("fpred_max_score")))
    .filter(col("fpred_max_score").isNotNull)
    .drop("score_label_map")

  variantGeneVepDf
}

def calculateDistanceDf(variant: DataFrame, gene: DataFrame, distance: Long): DataFrame = {

  val nearests = variant
    .select(
      Seq(
        lit("distance") as "type_id",
        lit("canonical_tss") as "source_id",
        lit("unspecified") as "feature"
      ) ++ idxCols: _*
    )

  val dists = gene
    .transform(Gene.variantGeneDistance(nearests, distance))
    .withColumn("distance_score", when(col("d") > 0, lit(1.0) / col("d")).otherwise(1.0))

  /** fixme: This feels wrong. We assign a score based on source_id and feature which are the same for all rows.
    */
  val w = Window.partitionBy("source_id", "feature").orderBy(col("distance_score").asc)

  dists
    .withColumn("distance_score_q", round(percent_rank().over(w), 1))
    .select(
      idxCols ++ Seq("d", "distance_score", "distance_score_q").map(col) :+ col("gene_id"): _*
    )
}

def calculateQtls(variant: DataFrame, qtls: DataFrame): DataFrame = {

  val qtl = qtls
    .select(
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
    )
    .withColumn("qtl_score", -log(10, col("qtl_pval")))
    .join(variant, idxStrs, "left_semi")

  val w = Window.partitionBy("source_id", "feature").orderBy(col("qtl_score").asc)

  qtl.withColumn("qtl_score_q", round(percent_rank().over(w), 1))
}

def calculateIntervals(variantRawDf: DataFrame, intervalRawDf: DataFrame): DataFrame = {
  val variantDf = variantRawDf.select("chr_id", "position", "ref_allele", "alt_allele")
  val intervalDf = intervalRawDf
    .withColumn("filename", input_file_name)
    .select(
      col("*"),
      split(split(col("filename"), "/interval/")(1), "/") as "file_metadata"
    )
    .select(
      col("gene_id"),
      col("chrom") as "chr_id",
      col("start"),
      col("end"),
      col("score"),
      col("bio_feature") as "feature",
      col("file_metadata")(0) as "type_id",
      col("file_metadata")(1) as "source_id",
    )
    .groupBy("chr_id", "start", "end", "gene_id", "type_id", "source_id", "feature")
    .agg(max(col("score")).as("interval_score"))

  val intervalWithVariantDf = intervalDf
    .join(variantDf.withColumnRenamed("chr_id", "chr"),
      col("chr_id") === col("chr") && col("position") > col("start") && col(
        "position") < col("end"))
    .drop("score", "start", "end", "chr")

  val w = Window.partitionBy("source_id", "feature").orderBy(col("interval_score").asc)

  intervalWithVariantDf
    .withColumn("interval_score_q", round(percent_rank().over(w), 1))

}

val variantRawDf: DataFrame = ss.read.parquet("/home/jarrod/genetics/output/variant/*.parquet")
val targetRawDf: DataFrame = Gene.getGeneDf(ss.read.parquet("/home/jarrod/genetics/output/target/*.parquet"))
val qtlRawDf: DataFrame = ss.read.parquet("/home/jarrod/genetics/v2g/qtl/*/*.parquet")
val vepRawDf: DataFrame = csqs("/home/jarrod/genetics/v2g/vep_consequences.tsv")
val intervalRawDf: DataFrame = ss.read.parquet("/home/jarrod/genetics/v2g/interval/*/*/*/data.parquet")
val tssDistance = 500000

val variantVep = calculateVep(variantRawDf, vepRawDf)
val variantDistance = calculateDistanceDf(variantRawDf, targetRawDf, tssDistance)
val variantQtl = calculateQtls(variantRawDf, qtlRawDf)
val variantInterval = calculateIntervals(variantRawDf, intervalRawDf)

val variantGeneIdx: DataFrame =
  unionDataframeDifferentSchema(Seq(variantVep, variantDistance, variantQtl, variantInterval))
    .join(targetRawDf, Seq("gene_id"), "left_semi")
