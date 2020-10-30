import $ivy.`ch.qos.logback:logback-classic:1.2.3`
import $ivy.`com.typesafe.scala-logging::scala-logging:3.9.2`
import $ivy.`com.typesafe:config:1.4.0`
import org.apache.spark.storage.StorageLevel
// import $ivy.`com.github.fommil.netlib:all:1.1.2`
import $ivy.`org.apache.spark::spark-core:3.0.1`
import $ivy.`org.apache.spark::spark-mllib:3.0.1`
import $ivy.`org.apache.spark::spark-sql:3.0.1`
import $ivy.`com.github.pathikrit::better-files:3.8.0`
import $ivy.`com.typesafe.play::play-json:2.9.1`
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import com.typesafe.scalalogging.LazyLogging

/**
  * export JAVA_OPTS="-Xms1G -Xmx24G"
  * time amm opentargets-epmc-entity-resolution.sc
  * --entitiesUri ../etl/mkarmona/association/tags_Annot_PMC1240624_PMC1474480_v01.jsonl
  * --lutsUri ../etl/mkarmona/association/search_\\*\\*\/part\\*.json
  * --outputUri mapped_entities/
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
  import ColumnTransformationHelpers._

  object ColumnTransformationHelpers {
    def normalise(c: Column): Column = {
      // https://www.rapidtables.com/math/symbols/greek_alphabet.html
      translate(rtrim(lower(translate(trim(trim(c), "."), "/`''[]{}()- ", "")), "s"),
                "αβγδεζηικλμνξπτυω",
                "abgdezhiklmnxptuo")
    }
  }

  def loadVCF(uri: String, cols: Seq[String])(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._

    val csvOptions = Map("sep" -> "\t",
                         "comment" -> "#",
                         "header" -> "false",
                         "inferSchema" -> "true",
                         "ignoreLeadingWhiteSpace" -> "true",
                         "ignoreTrailingWhiteSpace" -> "true")

    val chromosomes = (1 to 22).map(_.toString) ++ List("X", "Y")

    sparkSession.read
      .options(csvOptions)
      .csv(uri)
      .toDF(cols: _*)
      .filter($"chrom".isInCollection(chromosomes))
  }

  def loadClinvar(df: DataFrame)(
    implicit sparkSession: SparkSession): DataFrame = {

    import sparkSession.implicits._

    val oper = (c: Column, prefix: String) => split(filter(c, co => instr(co, prefix) > 0).getItem(0), "=").getItem(1)

    df.withColumn("_info", split(col("info"), ";"))
      .withColumn("clinicalSignificance", oper($"_info", "CLNSIG"))
      .withColumn("variantRsId", concat(lit("rs"), oper($"_info", "RS")))
      .withColumn("diseaseName", oper($"_info", "CLNDN"))
      .withColumn("geneSymbol", split(oper($"_info", "GENEINFO"), ":").getItem(0))
      .withColumn("variantId",
        concat_ws("_", $"chrom", $"pos", $"ref", $"alt"))
      .withColumn("variant", coalesce($"variantRsId", $"variantId"))
      .selectExpr("variant", "clinicalSignificance", "diseaseName", "geneSymbol")
      .repartition($"variant")
      .orderBy($"variant".asc)
  }

  def annotateVariants(snps: DataFrame, idx: DataFrame)(
      implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    snps
      .join(idx,
            $"chrom" === $"chr_id_b37" and
              $"pos" === $"position_b37" and
              $"ref" === $"ref_allele" and
              $"alt" === $"alt_allele",
            "inner")
      .withColumn("variant_id",
                  concat_ws("_", $"chr_id", $"position", $"ref_allele", $"alt_allele"))
      .withColumn("variant", coalesce($"rs_id", $"variant_id"))
      .repartition($"variant")
      .orderBy($"variant".asc)
  }

  def processWithClinvar(snps: DataFrame, clinvar: DataFrame)(
    implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    val annotatedEvs = snps
      .join(clinvar, Seq("variant"), "inner")
      .persist()
      .groupBy(col("geneSymbol"))
      .agg(collect_set(struct($"variant", $"variant_id", $"clinicalSignificance", $"diseaseName")).as(
        "variantDiseases"))
      .withColumn("variantDisease", explode(col("variantDiseases")))
      .drop("variantDiseases")

    annotatedEvs
  }

  def processWithEvidences(snps: DataFrame, evs: DataFrame)(
      implicit sparkSession: SparkSession): DataFrame = {
    val annotatedEvs = snps
      .join(evs, Seq("variant"), "inner")
      .persist()
      .groupBy(col("targetId"))
      .agg(collect_set(struct(col("variantId"), col("variantRsId"), col("diseaseId"))).as(
        "variantDiseases"))
      .withColumn("variantDisease", explode(col("variantDiseases")))
      .drop("variantDiseases")

    annotatedEvs
  }

  def apply(snpsUri: String, variantIndexUri: String, evidencesUri: String, clinvarUri: String, outputUri: String) = {
    implicit val spark = {
      val ss = SparkSessionWrapper.session
      ss.sparkContext.setLogLevel("WARN")
      ss
    }

    val minCols = List(
      "chrom",
      "pos",
      "id",
      "ref",
      "alt",
      "qual",
      "filter",
      "info"
    )

    val danteCols = minCols ++ List(
      "format",
      "GFX0237388"
    )
    val snps = loadVCF(snpsUri, danteCols).filter(col("filter") === "PASS")

    val clinSigList = List(
      "Pathogenic",
      "Likely_pathogenic",
      "Pathogenic/Likely_pathogenic",
      "risk_factor",
      "Likely_pathogenic,_risk_factor",
      "Pathogenic,_risk_factor",
      "Pathogenic/Likely_pathogenic,_risk_factor"
    )

    val clinvar = loadClinvar(loadVCF(clinvarUri, minCols))
      .filter(col("clinicalSignificance").isInCollection(clinSigList) and col("diseaseName") =!= "not_provided")
      .persist()

    clinvar.write.json("clinvarDataset")

    val variantIdx = spark.read.parquet(variantIndexUri)

//    val evidences = spark.read
//      .parquet(evidencesUri)
//      .filter(
//        col("datatypeId") === "genetic_association" and
//          (col("variantId").isNotNull or col("variantRsId").isNotNull) and
//          col("score") >= 0.5)
//      .withColumn("variant", coalesce(col("variantRsId"), col("variantId")))
//      .orderBy(col("variant").asc)
//      .persist()

    val annotatedVariants = annotateVariants(snps, variantIdx).persist()

    annotatedVariants.write.json("annotatedVariantsDataset")

//    val resolvedEntities = processWithEvidences(annotatedVariants, evidences)
    val resolvedEntities = processWithClinvar(annotatedVariants, clinvar)
    resolvedEntities.write.json(outputUri)
  }
}

@main
  def main(snpsUri: String,
           variantIndexUri: String,
           clinvarUri: String,
           evidencesUri: String,
           outputUri: String): Unit =
    ETL(snpsUri, variantIndexUri, evidencesUri, clinvarUri, outputUri)
