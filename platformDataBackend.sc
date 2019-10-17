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
      val genAncestors = udf((codes: Seq[Seq[String]]) =>
        codes.view.flatten.toSet.toSeq)

      val efos = df
        .withColumn("id", substring_index(col("code"), "/", -1))
        .withColumn("ancestors", genAncestors(col("path_codes")))
        .drop("paths", "private", "_private", "path")

      val descendants = efos
        .where(size(col("ancestors")) > 0)
        .withColumn("ancestor", explode(col("ancestors")))
        // all diseases have an ancestor, at least itself
        .groupBy("ancestor")
        .agg(collect_set(col("id")).as("descendants"))
        .withColumnRenamed("ancestor", "id")

      efos.join(descendants, Seq("id"))
        .drop("code", "children", "path_codes", "path_labels")
    }

    def setIdAndSelectFromDrugs(evidences: DataFrame): DataFrame = {
      def _getUniqTargetsAndDiseasesPerDrugId(evs: DataFrame): DataFrame = {
        evs.filter(col("sourceID") === "chembl")
          .withColumn("drug_id", substring_index(col("drug.id"), "/", -1))
          .groupBy(col("drug_id"))
          .agg(collect_set(col("target.id")).as("linkedTargets"),
            collect_set(col("disease.id")).as("linkedDiseases"))
          .withColumn("linkedTargetsCount", size(col("linkedTargets")))
          .withColumn("linkedDiseasesCount", size(col("linkedDiseases")))
      }

      val selectExpression = Seq(
        "id",
        "pref_name as name",
        "synonyms",
        "ifnull(trade_names, array()) as tradeNames",
        "year_first_approved as yearOfFirstApproval",
        "`type` as drugType",
        "max_clinical_trial_phase as maximumClinicalTrialPhase",
        "withdrawn_flag as hasBeenWithdrawn",
        "internal_compound as internalCompound",
        "struct(ifnull(linkedTargetsCount, 0) as count, ifnull(linkedTargets, array()) as rows) as linkedTargets",
        "struct(ifnull(linkedDiseasesCount,0) as count, ifnull(linkedDiseases,array()) as rows) as linkedDiseases",
        "withdrawnNotice")

      val mechanismsOfAction =
        """
          |struct(
          |  transform(mechanisms_of_action, m -> struct(m.description as mechanismOfAction,
          |    m.target_name as targetName,
          |    m.references as references,
          |    ifnull(array_distinct(
          |      transform(m.target_components, t -> t.ensembl)), array()) as targets)) as rows,
          |  array_distinct(transform(mechanisms_of_action, x -> x.action_type)) as uniqueActionTypes,
          |  array_distinct(transform(mechanisms_of_action, x -> x.target_type)) as uniqueTargetTypes) as mechanismsOfAction
          |""".stripMargin

      df.join(_getUniqTargetsAndDiseasesPerDrugId(evidences), col("id") === col("drug_id"), "left_outer")
        .withColumn("withdrawnNotice", when(col("withdrawn_class").isNull and col("withdrawn_country").isNull and
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
  val evidences = Loaders.loadEvidences(evidenceFilename)

  diseases
    .setIdAndSelectFromDiseases
    .write.json(outputPathPrefix + "/diseases/")

  targets
    .setIdAndSelectFromTargets
    .write.json(outputPathPrefix + "/targets/")

  drugs
    .setIdAndSelectFromDrugs(evidences)
    .write.json(outputPathPrefix + "/drugs/")
}
