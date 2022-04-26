package io.opentargets.etl.preprocess.uniprot

/** Description line (DE) contains additional information about protein names.
  *
  * See [[https://web.expasy.org/docs/userman.html#DE_line DE documentation]] for additional information.
  */
trait GeneIdentifiers {

  val SYMBOL_NAME = "Name"
  val SYMBOL_SYNONYMS = "Synonyms"
  val SYMBOL_ORF = "ORFNames"
  val genePrefixes: List[String] = SYMBOL_NAME :: SYMBOL_SYNONYMS :: SYMBOL_ORF :: Nil

  def processSymbolSynonyms(lines: Seq[String]): Seq[String] = {
    lines
      .mkString(" ")
      .split(";")
      .withFilter(_.nonEmpty)
      .map(_.trim)
      .groupBy(_.split("=").head)
      .filterKeys(genePrefixes.contains)
      .flatMap(pair => pair._2.flatMap(_.split("=").tail))
      .flatMap(_.split(","))
      .map(sym => sym.trim.stripSuffix(";").trim)
      .toSeq
  }
}
