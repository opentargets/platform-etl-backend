import $ivy.`ch.qos.logback:logback-classic:1.2.3`
import $ivy.`com.typesafe.scala-logging::scala-logging:3.9.2`
import $ivy.`com.typesafe:config:1.4.0`
import $ivy.`com.github.fommil.netlib:all:1.1.2`
import $ivy.`org.apache.spark::spark-core:2.4.5`
import $ivy.`org.apache.spark::spark-mllib:2.4.5`
import $ivy.`org.apache.spark::spark-sql:2.4.5`
import $ivy.`com.github.pathikrit::better-files:3.8.0`
import $ivy.`sh.almond::ammonite-spark:0.7.0`
import $ivy.`com.typesafe.play::play-json:2.8.1`
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.storage.StorageLevel

/**
  Spark common functions
  */
object SparkSessionWrapper extends LazyLogging {
  logger.info("Spark Session init")
  lazy val sparkConf = new SparkConf()
    .set("spark.driver.maxResultSize", "0")
    .set("spark.debug.maxToStringFields", "2000")
    .setAppName("etl-generation")
    .setMaster("local[*]")

  lazy val session: SparkSession =
    SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate
}

object ETL extends LazyLogging {
  def getGenes(uri: String)(implicit ss: SparkSession): DataFrame = {
    import ss.implicits._

    val columns = Seq(
      "ensembl_gene_id as gene_id",
      "approved_symbol as gene_symbol",
      "symbol_synonyms as gene_symbol_synonyms",
      "approved_name as gene_name",
      "uniprot_accessions as gene_uniprot_accessions",
      "uniprot_id as gene_uniprot_id",
      "uniprot_subcellular_location as gene_uniprot_subcellular_loc",
      "biotype as gene_biotype",
      "chromosome as gene_chr_id",
      "gene_start",
      "gene_end",
      "strand as gene_strand",
      "go as gene_annotated_go"
    )

    ss.read.parquet(uri).selectExpr(columns:_*)
  }

  def extractV2D(genes: DataFrame, v2dUri: String)(implicit ss: SparkSession) = {
    import ss.implicits._
    // build a whitelist of genes sorted by left hand range
    val genesChrPos = broadcast(genes
      .orderBy(col("gene_chr_id").asc, col("gene_left_range").asc)
    )

    val cols = Seq(
      "study_id",
      "trait_reported",
      "tag_chrom",
      "tag_pos"
    )

    // TODO include variants tag or lead
    val v2d = ss.read.json(v2dUri)
      .selectExpr(cols:_*)
      .orderBy($"tag_chrom".asc, $"tag_pos".asc)
      .join(genesChrPos, $"tag_chrom" === $"gene_chr_id" and
        $"tag_pos" >= $"gene_left_range" and $"tag_pos" <= $"gene_right_range",
        "inner")
      .withColumn("prioritised_study", struct(
        $"study_id",
        $"trait_reported"
      ))
      .groupBy($"gene_id")
      .agg(collect_set($"prioritised_study").as("prioritised_studies"))
      .orderBy($"gene_id".asc)

    v2d
  }

  def extractL2G(genes: DataFrame, l2gUri: String, studiesUri: String)(implicit ss: SparkSession) = {
    import ss.implicits._
    val gIdx = broadcast(genes
      .orderBy(col("gene_id").asc))

    val studies = ss.read.parquet(studiesUri).selectExpr(
      "study_id",
      "trait_reported"
    )

    val l2g = ss.read.parquet(l2gUri)
      .join(studies, Seq("study_id"), "inner")
      .orderBy(col("gene_id").asc)
      .join(gIdx, Seq("gene_id"), "left_semi")
      .withColumn("prioritised_locus", struct(
        $"study_id",
        $"trait_reported",
        $"y_proba_full_model"
      ))
      .groupBy(col("gene_id"))
      .agg(collect_set($"prioritised_locus").as("prioritised_loci"))

    l2g
  }

  def extractColoc(genes: DataFrame, colocUri: String, studiesUri: String)(implicit ss: SparkSession) = {
    import ss.implicits._
    val gIdx = broadcast(genes
      .orderBy(col("gene_id").asc))

    val studies = ss.read.parquet(studiesUri).selectExpr(
      "study_id",
      "trait_reported"
    )

    val coloc = ss.read.json(colocUri)
      .join(studies, $"left_study" === $"study_id" and $"left_type" === "gwas", "inner")
      .orderBy(col("right_gene_id").asc)
      .join(gIdx, $"right_gene_id" === $"gene_id" and $"left_type" === "gwas" and $"right_type" =!= "gwas", "left_semi")
      .withColumn("prioritised_coloc", struct(
        $"study_id",
        $"trait_reported",
        $"coloc_h3",
        $"coloc_h4"
      ))
      .groupBy(col("right_gene_id").as("gene_id"))
      .agg(collect_set($"prioritised_coloc").as("prioritised_colocs"))

    coloc
  }

  def computeGeneIdx(genes: DataFrame)(implicit ss: SparkSession) = {
    import ss.implicits._
    // note that we dont have here the tss so using the start of the gene
    val geneIdsLocs = genes.selectExpr(
      "gene_id",
      "gene_chr_id",
      "gene_start",
      "gene_end"
    )
      .withColumn("gene_left_range",
        when($"gene_start" - 1000000 < 0, 0)
          .otherwise($"gene_start" - 1000000))
      .withColumn("gene_right_range", $"gene_start" + 1000000)

    geneIdsLocs
  }

  def apply(genesUri: String,
            expressionUri: String,
            l2gUri: String,
            studiesUri: String,
            v2dUri: String,
            colocUri: String,
            outputUri: String) = {
    implicit val spark = SparkSessionWrapper.session

    val geneSet = getGenes(genesUri).orderBy(col("gene_id").asc).persist()
    val expression = spark.read.parquet(expressionUri)
      .withColumnRenamed("gene", "gene_id")
      .orderBy(col("gene_id").asc)

    val genesIdx = computeGeneIdx(geneSet).persist(StorageLevel.DISK_ONLY_2)
    val v2d = extractV2D(genesIdx, v2dUri).persist(StorageLevel.DISK_ONLY_2)
    val l2g = extractL2G(genesIdx, l2gUri, studiesUri).persist(StorageLevel.DISK_ONLY_2)
    val coloc = extractColoc(genesIdx, colocUri, studiesUri).persist(StorageLevel.DISK_ONLY_2)

    geneSet
      .join(l2g, Seq("gene_id"), "left_outer")
      .join(v2d, Seq("gene_id"))
      .join(coloc, Seq("gene_id"))
      .join(expression, Seq("gene_id"), "left_outer")
      .write.json(outputUri)
  }
}

@main
def main(genesUri: String,
         expressionUri: String,
         l2gUri: String,
         studiesUri: String,
         v2dUri: String,
         colocUri: String,
         outputUri: String): Unit =
  ETL(genesUri, expressionUri, l2gUri, studiesUri, v2dUri, colocUri, outputUri)
