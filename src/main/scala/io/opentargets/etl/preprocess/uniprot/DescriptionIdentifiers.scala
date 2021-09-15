package io.opentargets.etl.preprocess.uniprot

/**
  * Description line (DE) contains additional information about protein names.
  *
  * See [[https://web.expasy.org/docs/userman.html#DE_line DE documentation]] for additional information.
  */
trait DescriptionIdentifiers {

  val RECOMMENDED = "RecName: Full"
  val ALTERNATIVE = "AltName: Full"
  val ALTERNATIVE_ANTIGEN = "AltName: CD_antigen"
  val ALTERNATIVE_SHORT = "Short"

  case class UniprotProcessedNames(recNames: Seq[String],
                                   altNames: Seq[String],
                                   symbols: Seq[String])
  val prefixes
    : List[String] = RECOMMENDED :: ALTERNATIVE :: ALTERNATIVE_ANTIGEN :: ALTERNATIVE_SHORT :: Nil

  def processNames(descriptions: Seq[String]): UniprotProcessedNames = {
    lazy val uniMap = descriptions
      .map(_.split("=").map(_.trim))
      .groupBy(_.head)
      .filterKeys(k => prefixes.contains(k))
      .mapValues(_.flatMap(_.drop(1).map(n => n.split("\\{").head.trim.stripSuffix(";"))))
      .withDefaultValue(List.empty[String])

    UniprotProcessedNames(
      uniMap(RECOMMENDED),
      uniMap(ALTERNATIVE),
      uniMap(ALTERNATIVE_ANTIGEN) ++ uniMap(ALTERNATIVE_SHORT)
    )
  }

  val SYMBOL_NAME = "Name"
  val SYMBOL_SYNONYMS = "Synonyms"

  def processSymbolSynonyms(lines: Seq[String]): Seq[String] = {
    lines
      .map(l => l.split("="))
      .withFilter(w => (SYMBOL_SYNONYMS :: SYMBOL_NAME :: Nil).contains(w.head))
      .flatMap(g => g.drop(1).flatMap(_.split(",")).map(sym => sym.trim.split("\\{").head.trim))
  }
}
