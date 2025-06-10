package io.opentargets.etl.backend.drug

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, typedLit, udf, when}

/** Utility object to hold methods common to Drug and DrugBeta steps to prevent code duplication.
  */
object DrugCommon extends Serializable with LazyLogging {

  // Effectively a wrapper around the 'description` UDF: isolating in function so the adding/dumping necessary
  // columns doesn't clutter logic in the apply method. Note: this should be applied after all other transformations!
  def addDescription(dataFrame: DataFrame): DataFrame =
    dataFrame
      .withColumn("_indication_phases", col("indications.maxPhaseForIndication"))
      .withColumn("_indication_labels", col("indications.efoName"))
      .transform(addDescriptionField)
      .drop("_indication_phases", "_indication_labels")

  /*
  Adds description field to dataframe using UDF.
   */
  def addDescriptionField(dataFrame: DataFrame): DataFrame =
    dataFrame.withColumn(
      "description",
      DrugCommon.generateDescriptionFieldUdf(
        col("drugType"),
        when(col("maximumClinicalTrialPhase").isNotNull, col("maximumClinicalTrialPhase"))
          .otherwise(-1),
        when(col("yearOfFirstApproval").isNotNull, col("yearOfFirstApproval")).otherwise(0),
        when(col("_indication_phases").isNotNull, col("_indication_phases"))
          .otherwise(typedLit(Seq.empty[Double])),
        when(col("_indication_labels").isNotNull, col("_indication_labels"))
          .otherwise(typedLit(Seq.empty[String])),
        when(col("isWithdrawn").isNotNull, col("isWithdrawn")).otherwise(false),
        col("blackBoxWarning")
      )
    )

  /** User defined function wrapper of `generateDescriptionField`
    */
  val generateDescriptionFieldUdf: UserDefinedFunction = udf(
    (
        drugType: String,
        maxPhase: Double,
        firstApproval: Int,
        indicationPhases: Seq[Double],
        indicationLabels: Seq[String],
        isWithdrawn: Boolean,
        blackBoxWarning: Boolean
    ) =>
      generateDescriptionField(
        drugType,
        Option(maxPhase),
        if (firstApproval > 0) Some(firstApproval) else None,
        indicationPhases,
        indicationLabels,
        isWithdrawn,
        blackBoxWarning
      )
  )

  /** take a list of tokens and join them like a proper english sentence with items in it. As an
    * example ["miguel", "cinzia", "jarrod"] -> "miguel, cinzia and jarrod" and all the the
    * causistic you could find in it.
    * @param tokens
    *   list of tokens
    * @param start
    *   prefix string to use
    * @param sep
    *   the separator to use but not with the last two elements
    * @param end
    *   the suffix to put
    * @param lastSep
    *   the last separator as " and "
    * @tparam T
    *   it is converted to string
    * @return
    *   the unique string with all information concatenated
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

  /** Use drug metadata to construct a syntactically correct English sentence description of the
    * drug.
    * @return
    *   sentence describing the key features of the drug.
    */
  def generateDescriptionField(
      drugType: String,
      maxPhase: Option[Double],
      firstApproval: Option[Int],
      indicationPhases: Seq[Double],
      indicationLabels: Seq[String],
      isWithdrawn: Boolean,
      blackBoxWarning: Boolean,
      minIndicationsToShow: Int = 2
  ): String = {

    val romanNumbers =
      Map[Double, String](4.0 -> "IV", 3.0 -> "III", 2.0 -> "II", 1.0 -> "I", 0.5 -> "I (Early)")
        .withDefaultValue("")

    val mainNote = Some(s"${drugType.capitalize} drug")
    val phase = maxPhase match {
      case Some(-1) => None
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
    val approvedIndications = indications.filter(_._1 == 4.0)
    val investigationalIndicationsCount = indications.view.count(_._1 < 4.0)

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

    val wdrawnNote = isWithdrawn match {
      case true =>
        Some(" It was withdrawn in at least one region.")
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

}
