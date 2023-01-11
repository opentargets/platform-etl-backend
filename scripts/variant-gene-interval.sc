import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, concat_ws, input_file_name, max, percent_rank, round, split, when}

val ss: SparkSession = ???

import ss.implicits._
val variantRawDf = ss.read.parquet("/home/jarrod/development/platform-etl-backend/data/output/variant-index/part-00000-86ba7de0-1744-4189-b0a5-c6c644c4b04c-c000.snappy.parquet")
val intervalRawDf = ss.read.parquet("/home/jarrod/development/platform-etl-backend/data/genetics/vg/interval/pchic/jung2019/190920/part-00000-a358e032-08f6-40ed-a978-4e6bfc777476-c000.snappy.parquet")

val intervalRawDf = ss.read.parquet("/home/jarrod/genetics/interval/pchic/jung2019/190920/data/*.parquet")
val variantRawDf = ss.read.parquet("/home/jarrod/genetics/output/variant/part-00000-86ba7de0-1744-4189-b0a5-c6c644c4b04c-c000.snappy.parquet")

// gcloud
val variantRawDf = ss.read.parquet("/home/jarrod/variant-index")
val intervalRawDf = ss.read.parquet("/home/jarrod/interval/*/*/*/data.parquet/")

val variantDf = variantRawDf.select("chr_id", "position", "ref_allele", "alt_allele")
//  .repartition(col("chr_id"))

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
  .drop("score")
//  .repartition(col("chr_id"))

val w = Window.partitionBy("source_id", "feature").orderBy(col("interval_score").asc)

val df1 = intervalDf.join(variantDf,
    intervalDf("chr_id") === variantDf("chr_id") && col("position") > col("start") && col(
      "position") < col("end")).drop("start", "end")

df1.explain()

df1
  .withColumn("rank", round(percent_rank().over(w), 1))
  .withColumn("interval_score_q", when(col("interval_score").isNotNull, col("rank")))
  .drop("rank")

// We get 116 in the new dataset, and 114 in the production data for that variant.
val vg: DataFrame = ???
vg.withColumn("vid", concat_ws("_", 'chr_id, 'position, 'ref_allele, 'alt_allele)).select('vid)
  .filter('vid === "1_154453788_C_T")
  .show

/**
SELECT count(*) as c FROM `open-targets-genetics.genetics.variant_gene`
WHERE chr_id = '1' AND position = 154453788 AND ref_allele = 'C' AND alt_allele = 'T'
  */