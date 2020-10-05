package io.opentargets.etl.backend.drug_beta

import io.opentargets.etl.backend.DrugHelpers.mkStringSemantic
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, collect_set, size, substring_index, typedLit, udf, when}

/**
  * Utility object to hold methods common to Drug and DrugBeta steps to prevent code duplication.
  */
object DrugCommon {

  def addDescriptionField(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumn("description", DrugCommon.generateDescriptionFieldUdf(
      col("drugType"),
      col("maximumClinicalTrialPhase"),
      when(col("yearOfFirstApproval").isNotNull, col("yearOfFirstApproval")).otherwise(0),
      when(col("_indication_phases").isNotNull, col("_indication_phases"))
        .otherwise(typedLit(Seq.empty[Long])),
      when(col("_indication_labels").isNotNull, col("_indication_labels"))
        .otherwise(typedLit(Seq.empty[String])),
      when(col("withdrawnNotice.year").isNotNull, col("withdrawnNotice.year")).otherwise(0),
      when(col("withdrawnNotice.countries").isNotNull, col("withdrawnNotice.countries"))
        .otherwise(typedLit(Seq.empty[String])),
      when(col("withdrawnNotice.classes").isNotNull, col("withdrawnNotice.classes"))
        .otherwise(typedLit(Seq.empty[String])),
      col("blackBoxWarning")
    ))
  }
  /**
    * User defined function wrapper of `generateDescriptionField`
    */
  lazy val generateDescriptionFieldUdf = udf(
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
      generateDescriptionField(
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

  /**
    * Use drug metadata to construct a syntactically correct English sentence description of the drug.
    * @param drugType
    * @param maxPhase
    * @param firstApproval
    * @param indicationPhases
    * @param indicationLabels
    * @param withdrawnYear
    * @param withdrawnCountries
    * @param withdrawnReasons
    * @param blackBoxWarning
    * @param minIndicationsToShow
    * @return sentence describing the key features of the drug.
    */
  def generateDescriptionField(
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
                               ): String = {

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
            mkStringSemantic(approvedIndications.map(_._2), " and is indicated for ")
          else
            Some(s" and has $n approved indications")
        case (0, m) =>
          Some(s" and has $m investigational indication${if (m > 1) "s" else ""}")
        case (n, m) =>
          if (n <= minIndicationsToShow)
            mkStringSemantic(
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
      s" ${if (withdrawnCountries.size > 1) "initially" else ""} in ${y.toString}")
    val countries = mkStringSemantic(withdrawnCountries, " in ")
    val reasons = mkStringSemantic(withdrawnReasons, " due to ")
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

  def getUniqTargetsAndDiseasesPerDrugId(evidenceDf: DataFrame): DataFrame = {
    evidenceDf
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

}
