package io.opentargets.etl.preprocess.uniprot

/** Description line (DE) contains additional information about protein names.
  *
  * See [[https://web.expasy.org/docs/userman.html#DE_line DE documentation]] for additional information.
  */
trait DescriptionIdentifiers {

  val RECOMMENDED = "RecName: Full"
  val ALTERNATIVE = "AltName: Full"
  val ALTERNATIVE_ANTIGEN = "AltName: CD_antigen"
  val ALTERNATIVE_SHORT = "Short"

  case class UniprotProcessedNames(
      recNames: Seq[String],
      altNames: Seq[String],
      symbols: Seq[String]
  )
  val descriptionPrefixes: List[String] =
    RECOMMENDED :: ALTERNATIVE :: ALTERNATIVE_ANTIGEN :: ALTERNATIVE_SHORT :: Nil

  def processNames(descriptions: Seq[String]): UniprotProcessedNames = {
    lazy val uniMap = descriptions
      .map(_.split("=").map(_.trim))
      .groupBy(_.head)
      .filterKeys(k => descriptionPrefixes.contains(k))
      .mapValues(_.flatMap(_.drop(1).map(_.trim.stripSuffix(";").trim)))
      .withDefaultValue(List.empty[String])

    UniprotProcessedNames(
      uniMap(RECOMMENDED),
      uniMap(ALTERNATIVE),
      uniMap(ALTERNATIVE_ANTIGEN) ++ uniMap(ALTERNATIVE_SHORT)
    )
  }
}
