package io.opentargets.etl.backend

import org.apache.spark.SparkConf
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.typesafe.config.Config
import io.opentargets.etl.backend.SparkHelpers.{IOResourceConfig, IOResources}
import io.opentargets.etl.backend.drug_beta.DrugCommon

import scala.collection.mutable.WrappedArray

object DrugHelpers extends Serializable {

  implicit class AggregationHelpers(df: DataFrame)(implicit ss: SparkSession) {
    import Configuration._
    import ss.implicits._

    // Method used in the main and in platformETL.sc
    def drugIndex(evidences: DataFrame): DataFrame = {
      val dfDrug = df.setIdAndSelectFromDrugs(evidences)
      dfDrug
    }

    def setIdAndSelectFromDrugs(evidences: DataFrame): DataFrame = {

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
        "transform(indications, i -> i.max_phase_for_indication) as _indication_phases",
        "transform(indications, i -> i.efo_label) as _indication_labels",
        "withdrawnNotice",
        "black_box_warning as blackBoxWarning"
      )
      // drug rewrite: done
      val mechanismsOfAction =
        """
          |if(number_of_mechanisms_of_action > 0,struct(
          |  transform(mechanisms_of_action, m -> struct(
          |    m.description as mechanismOfAction,
          |    m.target_name as targetName,
          |    m.references as references,
          |    ifnull(array_distinct(
          |      transform(m.target_components, t -> t.ensembl)), array()) as targets)) as rows,
          |  array_distinct(transform(mechanisms_of_action, x -> x.action_type)) as uniqueActionTypes,
          |  array_distinct(transform(mechanisms_of_action, x -> x.target_type)) as uniqueTargetTypes), null) as mechanismsOfAction
          |""".stripMargin

      // drug rewrite: done
      val indications =
        """
          |if(number_of_indications > 0,struct(
          |  transform(indications, m -> struct(m.efo_id as disease,
          |    m.max_phase_for_indication as maxPhaseForIndication,
          |    m.references as references)) as rows,
          |  number_of_indications as count), null) as indications
          |""".stripMargin

      df.join(
          DrugCommon.getUniqTargetsAndDiseasesPerDrugId(evidences),
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
        .selectExpr(selectExpression ++ Seq(mechanismsOfAction, indications): _*)
        .transform(DrugCommon.addDescriptionField)
        .drop("_indication_phases", "_indication_labels")
    }
  }
}

// This is option/step drug in the config file
object Drug extends LazyLogging {
  def apply()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession
    import ss.implicits._
    import DrugHelpers._

    val common = context.configuration.common
    val mappedInputs = Map(
      "drug" -> IOResourceConfig(common.inputs.drug.format, common.inputs.drug.path),
      "evidence" -> IOResourceConfig(
        common.inputs.evidence.format,
        common.inputs.evidence.path
      )
    )
    val inputDataFrame = SparkHelpers.readFrom(mappedInputs)

    val dfDrugIndex = inputDataFrame("drug")
      .setIdAndSelectFromDrugs(inputDataFrame("evidence"))

    val outputs = Seq("drugs")

    val outputConfs =
      SparkHelpers.generateDefaultIoOutputConfiguration(outputs: _*)(context.configuration)

    val outputDFs = (outputs zip Seq(dfDrugIndex)).toMap
    SparkHelpers.writeTo(outputConfs, outputDFs)
  }
}

