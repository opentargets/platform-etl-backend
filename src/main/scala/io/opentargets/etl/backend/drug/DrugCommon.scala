package io.opentargets.etl.backend.drug

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{
  col,
  collect_set,
  size,
  struct,
  substring_index,
  typedLit,
  udf,
  when
}

/** Utility object to hold methods common to Drug and DrugBeta steps to prevent code duplication.
  */
object DrugCommon extends Serializable with LazyLogging {

  // Effectively a wrapper around the 'description` UDF: isolating in function so the adding/dumping necessary
  // columns doesn't clutter logic in the apply method. Note: this should be applied after all other transformations!
  def addDescription(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .withColumn("_indication_phases", col("indications.maxPhaseForIndication"))
      .withColumn("_indication_labels", col("indications.efoName"))
      .transform(addDescriptionField)
      .drop("_indication_phases", "_indication_labels")
  }

  /*
  Adds description field to dataframe using UDF.
   */
  def addDescriptionField(dataFrame: DataFrame): DataFrame = {

    dataFrame.withColumn(
      "description",
      DrugCommon.generateDescriptionFieldUdf(
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
      )
    )
  }

  /** User defined function wrapper of `generateDescriptionField`
    */
  val generateDescriptionFieldUdf: UserDefinedFunction = udf(
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

  /** take a list of tokens and join them like a proper english sentence with items in it. As
    * an example ["miguel", "cinzia", "jarrod"] -> "miguel, cinzia and jarrod" and all the
    * the causistic you could find in it.
    * @param tokens list of tokens
    * @param start prefix string to use
    * @param sep the separator to use but not with the last two elements
    * @param end the suffix to put
    * @param lastSep the last separator as " and "
    * @tparam T it is converted to string
    * @return the unique string with all information concatenated
    */
  def mkStringSemantic[T](
      tokens: Option[Seq[T]],
      start: String = "",
      sep: String = ", ",
      end: String = "",
      lastSep: String = " and "
  ): Option[String] = {

    // nulls are quite diff to spot
    val strTokens: Seq[String] = tokens
      .map(
        _.withFilter(_ != null)
          .map(_.toString)
      )
      .getOrElse(Seq.empty[String])

    strTokens.size match {
      case 0 => None
      case 1 => Some(strTokens.mkString(start, sep, end))
      case _ =>
        Some(
          (Seq(strTokens.init.mkString(start, sep, "")) :+ lastSep :+ strTokens.last)
            .mkString("", "", end)
        )
    }
  }

  /** Use drug metadata to construct a syntactically correct English sentence description of the drug.
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
          if (n <= minIndicationsToShow) {
            DrugCommon.mkStringSemantic(
              Option(approvedIndications.map(_._2)),
              " and is indicated for "
            )
          } else
            Some(s" and has $n approved indications")
        case (0, m) =>
          Some(s" and has $m investigational indication${if (m > 1) "s" else ""}")
        case (n, m) =>
          if (n <= minIndicationsToShow)
            DrugCommon.mkStringSemantic(
              Option(approvedIndications.map(_._2)),
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
    val countries = DrugCommon.mkStringSemantic(Option(withdrawnCountries), " in ")
    val reasons = DrugCommon.mkStringSemantic(Option(withdrawnReasons), " due to ")
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
      .filter(col("sourceId") === "chembl")
      .groupBy(col("drugId"))
      .agg(
        collect_set(col("targetId")).as("targets"),
        collect_set(col("diseaseId")).as("diseases")
      )
      .withColumn("targetCount", size(col("targets")))
      .withColumn("diseaseCount", size(col("diseases")))
      .withColumn(
        "linkedTargets",
        struct(col("targets").as("rows"), col("targetCount").as("count"))
      )
      .withColumn(
        "linkedDiseases",
        struct(col("diseases").as("rows"), col("diseaseCount").as("count"))
      )
      .select("drugId", "linkedTargets", "linkedDiseases")
  }

}
