import $ivy.`com.typesafe:config:1.3.4`
import $ivy.`com.github.fommil.netlib:all:1.1.2`
import $ivy.`org.apache.spark::spark-core:2.4.3`
import $ivy.`org.apache.spark::spark-mllib:2.4.3`
import $ivy.`org.apache.spark::spark-sql:2.4.3`
import $ivy.`com.github.pathikrit::better-files:3.8.0`
import $ivy.`sh.almond::ammonite-spark:0.7.0`
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Loaders {
  /** load a drug datasset from OpenTargets drug index dump */
  def loadDrugs(path: String)(implicit ss: SparkSession): DataFrame = {
    val drugList = ss.read.json(path)
    drugList
  }

  def loadTargets(path: String)(implicit ss: SparkSession): DataFrame = {
    val targetList = ss.read.json(path)
    targetList
  }

  def loadDiseases(path: String)(implicit ss: SparkSession): DataFrame = {
    val diseaseList = ss.read.json(path)
    diseaseList
  }

  def loadEvidences(path: String)(implicit ss: SparkSession): DataFrame = {
    val evidences = ss.read.json(path)
    evidences
  }
}

object Transformers {
  implicit class Implicits (val df: DataFrame) {
    def setIdAndSelectFromTargets: DataFrame = {
      val selectExpressions = Seq(
        "id",
        "approved_name as approvedName",
        "approved_symbol as approvedSymbol",
        "biotype as bioType",
        "case when (hgnc_id = '') then null else hgnc_id end as hgncId",
        "name_synonyms as nameSynonyms",
        "symbol_synonyms as symbolSynonyms",
        "struct(chromosome, gene_start as start, gene_end as end, strand) as genomicLocation")

      val uniprotStructure = """
        |case
        |  when (uniprot_id = '' or uniprot_id = null) then null
        |  else struct(uniprot_id as id,
        |    uniprot_accessions as accessions,
        |    uniprot_function as functions)
        |end as proteinAnnotations
        |""".stripMargin

      df.selectExpr(selectExpressions :+ uniprotStructure:_*)
    }

    def setIdAndSelectFromDiseases: DataFrame = {
      df.withColumn("id", substring_index(col("code"), "/", -1))
        .drop("code")
    }

    def setIdAndSelectFromDrugs: DataFrame = {
      val selectExpression = Seq(
        "id",
        "pref_name as name",
        "synonyms",
        "trade_names as tradeNames",
        "year_first_approved as yearOfFirstApproval",
        "`type` as drugType",
        "max_clinical_trial_phase as maximumClinicalTrialPhase",
        "withdrawn_flag as hasBeenWithdrawn",
        "internal_compound as internalCompound",
        "withdrawnNotice")

      val mechanismsOfAction =
        """
          |struct(
          |  transform(mechanisms_of_action, m -> struct(m.description as mechanismOfAction,
          |    m.target_name as targetName,
          |    m.references as references,
          |    array_distinct(
          |      transform(m.target_components, t -> t.ensembl)) as targets)) as rows,
          |  array_distinct(transform(mechanisms_of_action, x -> x.action_type)) as uniqueActionTypes,
          |  array_distinct(transform(mechanisms_of_action, x -> x.target_type)) as uniqueTargetTypes,
          |  flatten(transform(mechanisms_of_action, x -> x.references)) as references) as mechanismsOfAction
          |""".stripMargin

      df.withColumn("withdrawnNotice", when(col("withdrawn_class").isNull and col("withdrawn_country").isNull and
        col("withdrawn_year").isNull, lit(null))
        .otherwise(struct(col("withdrawn_class").as("classes"),
          col("withdrawn_country").as("countries"),
          col("withdrawn_year").as("year"))))
        .selectExpr(selectExpression ++ Seq(mechanismsOfAction):_*)
    }
  }
}

@main
def main(drugFilename: String, targetFilename: String, diseaseFilename: String,
         evidenceFilename: String, outputPathPrefix: String): Unit = {
  val sparkConf = new SparkConf()
    .set("spark.driver.maxResultSize", "0")
    .setAppName("similarities-loaders")
    .setMaster("local[*]")

  implicit val ss = SparkSession.builder
    .config(sparkConf)
    .getOrCreate

  // AmmoniteSparkSession.sync()

  import ss.implicits._
  import Transformers.Implicits

  val drugs = Loaders.loadDrugs(drugFilename)
  val targets = Loaders.loadTargets(targetFilename)
  val diseases = Loaders.loadDiseases(diseaseFilename)
  //  val evidences = Loaders.loadEvidences(evidenceFilename)

  diseases
    .setIdAndSelectFromDiseases
    .write.json(outputPathPrefix + "/diseases/")

  targets
    .setIdAndSelectFromTargets
    .write.json(outputPathPrefix + "/targets/")

  drugs
    .setIdAndSelectFromDrugs
    .write.json(outputPathPrefix + "/drugs/")
}
