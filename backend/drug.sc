import $file.common
import common._

import $ivy.`ch.qos.logback:logback-classic:1.2.3`
import $ivy.`com.typesafe.scala-logging::scala-logging:3.9.2`
import $ivy.`com.typesafe:config:1.4.0`
import $ivy.`com.github.fommil.netlib:all:1.1.2`
import $ivy.`org.apache.spark::spark-core:2.4.3`
import $ivy.`org.apache.spark::spark-mllib:2.4.3`
import $ivy.`org.apache.spark::spark-sql:2.4.3`
import $ivy.`com.github.pathikrit::better-files:3.8.0`
import $ivy.`sh.almond::ammonite-spark:0.7.0`
import org.apache.spark.SparkConf
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object DrugTransformers extends LazyLogging {

  implicit class ImplicitsDrug(val df: DataFrame) {
	  
	// Method used in the main and in platformETL.sc
    def drugIndex(evidences: DataFrame): DataFrame = {
      val dfDrug = df.setIdAndSelectFromDrugs(evidences)
      dfDrug
    }

    def setIdAndSelectFromDrugs(evidences: DataFrame): DataFrame = {
      def _getUniqTargetsAndDiseasesPerDrugId(evs: DataFrame): DataFrame = {
        evs
          .filter(col("sourceID") === "chembl")
          .withColumn("drug_id", substring_index(col("drug.id"), "/", -1))
          .groupBy(col("drug_id"))
          .agg(
            collect_set(col("target.id")).as("linkedTargets"),
            collect_set(col("disease.id")).as("linkedDiseases")
          )
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
        "withdrawnNotice"
      )

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

      df.join(
          _getUniqTargetsAndDiseasesPerDrugId(evidences),
          col("id") === col("drug_id"),
          "left_outer"
        )
        .withColumn(
          "withdrawnNotice",
          when(
            col("withdrawn_class").isNull and col("withdrawn_country").isNull and
              col("withdrawn_year").isNull,
            lit(null)
          ).otherwise(
            struct(
              col("withdrawn_class").as("classes"),
              col("withdrawn_country").as("countries"),
              col("withdrawn_year").as("year")
            )
          )
        )
        .selectExpr(selectExpression ++ Seq(mechanismsOfAction): _*)
    }
  }
}

// This is option/step drug in the config file
@main
def main(outputPathPrefix: String = "output"): Unit = {
  import DrugTransformers.ImplicitsDrug

  val cfg = Configuration.load
  val inputs = Configuration.loadInputs(cfg)

  val mappedInputs = Map(
    "drug" -> inputs.drug,
    "evidence" -> inputs.evidence
  )

  val inputDataFrame = SparkSessionWrapper.loader(mappedInputs)
  val dfDrugIndex = inputDataFrame("drug").drugIndex(inputDataFrame("evidence"))
  SparkSessionWrapper.save(dfDrugIndex, outputPathPrefix + "/drugs")

}
