import $file.common
import common._

import org.apache.spark.SparkConf
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.typesafe.config.Config
import scala.collection.mutable.WrappedArray

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
      def _generateDescriptionField(
          drugType: String,
          maxPhase: Option[Int],
          firstApproval: Option[Int],
          indicationPhases: Seq[Long],
          indicationLabels: Seq[String],
          withdrawnYear: Option[Int],
          withdrawnCountries: Seq[String],
          withdrawnReasons: Seq[String],
          blackBoxWarning: Boolean,
          minIndicationsToShow: Int = 2
      ) = {

        val romanNumbers = Map[Int, String](4 -> "IV", 3 -> "III", 2 -> "II", 1 -> "I", 0 -> "I")
          .withDefaultValue("")

        val mainNote = Some(s"${drugType.capitalize} drug")
        val phase = maxPhase match {
          case Some(p) =>
            Some(
              s" with a maximum clinical trial phase of ${romanNumbers(p)}${if (indicationLabels.size > 1) " (across all indications)"
              else ""}"
            )
          case _ => None
        }

        val approvedYear = firstApproval
          .map(y => s" that was first approved in $y")

        val indications = (indicationPhases zip indicationLabels).distinct
        val approvedIndications = indications.filter(_._1 == 4)
        val investigationalIndicationsCount = indications.view.count(_._1 < 4)

        val indicationsSentence =
          (approvedIndications.size, investigationalIndicationsCount) match {
            case (0, 0) =>
              None
            case (n, 0) =>
              if (n <= minIndicationsToShow)
                Helpers.mkStringSemantic(approvedIndications.map(_._2), " and is indicated for ")
              else
                Some(s" and has $n approved indications")
            case (0, m) =>
              Some(s" and has $m investigational indication${if (m > 1) "s" else ""}")
            case (n, m) =>
              if (n <= minIndicationsToShow)
                Helpers.mkStringSemantic(
                  approvedIndications.map(_._2),
                  start = " and is indicated for ",
                  end = s" and has $m investigational indication${if (m > 1) "s" else ""}"
                )
              else
                Some(
                  s" and has $n approved and $m investigational indication${if (m > 1) "s" else ""}"
                )
          }

        val mainSentence =
          Some(
            Seq(mainNote, phase, approvedYear, indicationsSentence, Some("."))
              .withFilter(_.isDefined)
              .map(_.get)
              .mkString
          )

        val year = withdrawnYear.map(y =>
          s" ${if (withdrawnCountries.size > 1) "initially" else ""} in ${y.toString}"
        )
        val countries = Helpers.mkStringSemantic(withdrawnCountries, " in ")
        val reasons = Helpers.mkStringSemantic(withdrawnReasons, " due to ")
        val wdrawnNoteList = List(Some(" It was withdrawn"), countries, year, reasons, Some("."))

        val wdrawnNote = wdrawnNoteList.count(_.isDefined) match {
          case n if n > 2 =>
            Some(wdrawnNoteList.withFilter(_.isDefined).map(_.get).mkString)
          case _ => None
        }

        val blackBoxWarningStr = if (blackBoxWarning) {
          Some(" This drug has a black box warning from the FDA.")
        } else {
          None
        }

        List(
          mainSentence,
          wdrawnNote,
          blackBoxWarningStr
        ).withFilter(_.isDefined).map(_.get).mkString
      }

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
        "transform(indications, i -> i.max_phase_for_indication) as _indication_phases",
        "transform(indications, i -> i.efo_label) as _indication_labels",
        "struct(ifnull(linkedTargetsCount, 0) as count, ifnull(linkedTargets, array()) as rows) as linkedTargets",
        "struct(ifnull(linkedDiseasesCount,0) as count, ifnull(linkedDiseases,array()) as rows) as linkedDiseases",
        "withdrawnNotice",
        "black_box_warning as blackBoxWarning"
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

      val descriptionFn = udf(
        (
            drugType: String,
            maxPhase: Int,
            firstApproval: Int,
            indicationPhases: Seq[Long],
            indicationLabels: Seq[String],
            withdrawnYear: Int,
            withdrawnCountries: Seq[String],
            withdrawnReasons: Seq[String],
            blackBoxWarning: Boolean
        ) =>
          _generateDescriptionField(
            drugType,
            Option(maxPhase),
            if (firstApproval > 0) Some(firstApproval) else None,
            indicationPhases,
            indicationLabels,
            if (withdrawnYear > 0) Some(withdrawnYear) else None,
            withdrawnCountries,
            withdrawnReasons,
            blackBoxWarning
          )
      )

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
        .selectExpr(selectExpression ++ Seq(mechanismsOfAction, indications): _*)
        .withColumn(
          "description",
          descriptionFn(
            $"drugType",
            $"maximumClinicalTrialPhase",
            when($"yearOfFirstApproval".isNotNull, $"yearOfFirstApproval").otherwise(0),
            when($"_indication_phases".isNotNull, $"_indication_phases")
              .otherwise(typedLit(Seq.empty[Long])),
            when($"_indication_labels".isNotNull, $"_indication_labels")
              .otherwise(typedLit(Seq.empty[String])),
            when($"withdrawnNotice.year".isNotNull, $"withdrawnNotice.year").otherwise(0),
            when($"withdrawnNotice.countries".isNotNull, $"withdrawnNotice.countries")
              .otherwise(typedLit(Seq.empty[String])),
            when($"withdrawnNotice.classes".isNotNull, $"withdrawnNotice.classes")
              .otherwise(typedLit(Seq.empty[String])),
            $"blackBoxWarning"
          )
        )
        .drop("_indication_phases", "_indication_labels")

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
      "drug" -> Map("format" -> common.inputs.drug.format, "path" -> common.inputs.drug.path),
      "evidence" -> Map(
        "format" -> common.inputs.evidence.format,
        "path" -> common.inputs.evidence.path
      )
    )
    val inputDataFrame = SparkSessionWrapper.loader(mappedInputs)

    val dfDrugIndex = inputDataFrame("drug")
      .setIdAndSelectFromDrugs(inputDataFrame("evidence"))

    dfDrugIndex.write.json(common.output + "/drugs")
  }
}
