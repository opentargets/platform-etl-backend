import org.apache.spark.sql.functions.{map_from_entries, _}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession}

implicit val ss: SparkSession = ???
import ss.implicits._

val targetRawDf = ss.read.parquet("/home/jarrod/genetics/targets/*.parquet")
//val targetRawDf = ss.read.parquet("/home/jarrod/development/platform-etl-backend/data/genetics/variant/target/*.parquet")
//val variantRawDf = ss.read.parquet("/home/jarrod/development/platform-etl-backend/data/genetics/variant/part-00000-2f1d26b6-a5b6-428f-9e62-affcd1ef6971-c000.snappy.parquet")
val variantRawDf = ss.read.parquet("/home/jarrod/genetics/part-00000*.parquet")


val approvedBioTypes = Set(
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
val excludedChromosomes: Set[String] = Set("MT")
val targetDf = targetRawDf
  .select(
    col("id") as "gene_id",
    col("genomicLocation.*"),
    col("biotype"),
    when(col("genomicLocation.strand") > 0, col("genomicLocation.start"))
      .otherwise(col("genomicLocation.end")) as "tss"
  )
  .filter(
    (col("biotype") isInCollection approvedBioTypes) && !(col("chromosome") isInCollection excludedChromosomes))
val proteinCodingDf = targetDf.filter(col("biotype") === "protein_coding")
val variantDf = variantRawDf
  .filter(col("chrom_b38").isNotNull && col("pos_b38").isNotNull)
  .select(
    col("chrom_b37") as "chr_id_b37",
    col("pos_b37") as "position_b37",
    col("chrom_b38") as "chr_id",
    col("pos_b38") as "position",
    col("ref") as "ref_allele",
    col("alt") as "alt_allele",
    col("rsid") as "rs_id",
    col("vep.most_severe_consequence") as "most_severe_consequence",
    col("cadd") as "cadd",
    col("af") as "af",
  )

def variantGeneDistance(target: DataFrame) =
  variantDf
    .join(target,
      (col("chr_id") === col("chromosome")) && (abs(col("position") - col("tss")) <= 500000))
    .withColumn("d", abs(col("position") - col("tss")))

val variantGeneDistanceDf = variantGeneDistance(targetDf)
val variantPcDistanceDf = variantGeneDistance(proteinCodingDf)
val variantId = Seq("chr_id", "position", "ref_allele", "alt_allele")
val variantGeneScored = variantGeneDistanceDf
  .groupBy(col("chr_id"),
           col("position"),
           col("ref_allele"),
           col("alt_allele"),
           col("gene_id") as "gene_id_any")
  .agg(min(col("d")) as "gene_id_any_distance")
  .distinct
val idStrings = Seq("chr_id", "position", "ref_allele", "alt_allele")
val variantIdCol = idStrings.map(col)

def findNearestGene(name: String)(df: DataFrame): DataFrame = {
  val nameDistance = s"${name}_distance"
  df.groupBy(variantIdCol: _*)
    .agg(collect_list(col("gene_id")) as "geneList",
      collect_list(col("d")) as "dist",
      min(col("d")) cast LongType as nameDistance)
    .select(
      variantIdCol ++ Seq(
        col(nameDistance),
        map_from_entries(arrays_zip(col("dist"), col("geneList"))) as "distToGeneMap"): _*)
    .withColumn(name, col("distToGeneMap")(col(nameDistance)))
    .drop("distToGeneMap", "geneList", "dist")
}

val variantPcScored = variantPcDistanceDf
  .groupBy(variantIdCol: _*)
  .agg(collect_list(col("gene_id")) as "geneList",
       collect_list(col("d") cast LongType) as "dist",
       min(col("d")) cast LongType as "gene_id_prot_coding_distance")
  .select(
    variantIdCol ++ Seq(
      col("gene_id_prot_coding_distance"),
      map_from_entries(arrays_zip(col("dist"), col("geneList"))) as "distToGeneMap"): _*)
  .withColumn("gene_id_prot_coding", col("distToGeneMap")(col("gene_id_prot_coding_distance")))
  .cache()

variantPcDistanceDf.transform(findNearestGene("gene_id_prot_coding"))

val vDistCount = variantPcDistanceDf
  .withColumn("id",
              concat(col("chr_id"),
                     lit("_"),
                     col("position"),
                     lit("_"),
                     col("ref_allele"),
                     lit("_"),
                     col("alt_allele")))
  .groupBy(col("id"))
  .agg(collect_list(col("gene_id")) as "geneList",
       collect_list(col("d") cast LongType) as "dist",
       min(col("d")) cast LongType as "gene_id_prot_coding_distance")
  .select(col("id"), explode(col("dist")) as "d")
  .groupBy("id", "d")
  .count
  .filter(col("count") > 1)

def addId(df: DataFrame): DataFrame = {
  df.withColumn("id",
    concat(col("chr_id"),
      lit("_"),
      col("position"),
      lit("_"),
      col("ref_allele"),
      lit("_"),
      col("alt_allele")))
}
