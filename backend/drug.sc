import $file.common
import common._

import org.apache.spark.SparkConf
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.typesafe.config.Config

object DrugHelpers {
  implicit class AggregationHelpers(df: DataFrame)(implicit ss: SparkSession) {
    import Configuration._
    import ss.implicits._

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
          |if(number_of_mechanisms_of_action > 0,struct(
          |  transform(mechanisms_of_action, m -> struct(m.description as mechanismOfAction,
          |    m.target_name as targetName,
          |    m.references as references,
          |    ifnull(array_distinct(
          |      transform(m.target_components, t -> t.ensembl)), array()) as targets)) as rows,
          |  array_distinct(transform(mechanisms_of_action, x -> x.action_type)) as uniqueActionTypes,
          |  array_distinct(transform(mechanisms_of_action, x -> x.target_type)) as uniqueTargetTypes), null) as mechanismsOfAction
          |""".stripMargin

      val indications =
        """
          |if(number_of_indications > 0,struct(
          |  transform(indications, m -> struct(m.efo_id as disease,
          |    m.max_phase_for_indication as maxPhaseForIndication,
          |    m.references as references)) as rows,
          |  number_of_indications as count), null) as indications
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
      /*

    // TODO
    AddFields(
      Field("descriptions", StringType,
        description = None,
        resolve = ctx => {
          /*
          Small molecule drug with a maximum clinical trial phase of IV (4)
          that was first approved in 1999 and has 14 approved or investigational
          indications. It was withdrawn in
          the United States in 2004 due to risk for heart attack and stroke.
          Drug category: Approved, Investigational, Withdrawn

          Antibody drug with a maximum clinical trial phase of IV (4)
          that was first approved in 1999 and has 39 approved or investigational indications.
          It was withdrawn in the European Union in 2010 due to increased risk of ischaemic
          heart disease. This drug has a black box warning from the FDA.
          Drug category: Approved, Investigational, Withdrawn, Black Box Warning

          Small molecule drug with a maximum clinical trial phase of I (1) and has
          8 investigational or approved indications.
           */
          val romanNumbers = Map(4 -> "IV", 3 -> "III", 2 -> "II", 1 -> "I").withDefaultValue("")

          val mainNote = Some(s"${ctx.value.drugType.capitalize()} drug")
          val maxPhase = ctx.value.maximumClinicalTrialPhase match {
            case Some(p) =>
              if (p == 0)
                None
              else
                Some(s" with a maximum clinical trial phase of ${romanNumbers(p)} (${p.toString})")
            case _ => None
          }

          val approvedYear = ctx.value.yearOfFirstApproval
            .map(" that was first approved in " + _.toString)

          // and has 39 approved or investigational indications
          val indications = ctx.value.indications.map(in => {
            in.count match {
              case n if (n <= 4) =>
                s" and is indicated for ${in.rows.init.map(_.disease)
                } and "
              case x =>
                s" and has ${x.toString} approved or investigational indications"
            }
          })
          val wdrawnNote = ctx.value.withdrawnNotice.map(wn => {
            val year = wn.year.map(y => s" in ${y.toString}")
            val countries = mkStringSemantical(wn.countries)
            val reasons = wn.reasons.map(_.mkString(" due to ", ", ", ""))
            List(Some("It was withdrawn"), countries, year, reasons)
              .withFilter(_.isDefined).map(_.get).mkString
          })
          val blackBoxWarning = ctx.value.blackBoxWarning match {
            case true => Some("<em>This drug has a black box warning from the FDA.</em>")
            case _ => None
          }

          val isApproved = None
          val isInvestigational = None
          val isWithdrawn = None
          val isBlackBoxWarning = None

          List(mainNote, maxPhase, approvedYear, Some("."),
            wdrawnNote, Some("."),
            blackBoxWarning)
            .withFilter(_.isDefined).map(_.get).mkString
        }
      )
    )
       */
        .withColumn("description", expr(""))
        .selectExpr(selectExpression ++ Seq(mechanismsOfAction, indications, "description"): _*)
    }
  }
}

// This is option/step drug in the config file
object Drug extends LazyLogging {
  def apply(config: Config)(implicit ss: SparkSession) = {
    import ss.implicits._
    import DrugHelpers._

    val common = Configuration.loadCommon(config)
    val mappedInputs = Map(
      "drug" -> common.inputs.drug,
      "evidence" -> common.inputs.evidence
    )
    val inputDataFrame = SparkSessionWrapper.loader(mappedInputs)

    val dfDrugIndex = inputDataFrame("drug")
      .setIdAndSelectFromDrugs(inputDataFrame("evidence"))

    SparkSessionWrapper.save(dfDrugIndex, common.output + "/drugs")

  }
}
