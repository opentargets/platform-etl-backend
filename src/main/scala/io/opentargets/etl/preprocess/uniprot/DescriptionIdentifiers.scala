package io.opentargets.etl.preprocess.uniprot

/**
  * Description line (DE) contains additional information about protein names.
  *
  * See [[https://web.expasy.org/docs/userman.html#DE_line DE documentation]] for additional information.
  */
trait DescriptionIdentifiers {

  val RECOMMENDED = "RecName:"
  val ALTERNATIVE = "AltName:"

  def processNames(descriptions: Seq[String]): (Seq[String], Seq[String]) = {
    val removeLeadingMetadata: String => String = (s: String) => s.split("=").last

    def processLines(lines: Seq[String]): Seq[String] =
      lines.map(removeLeadingMetadata.andThen(_.dropRight(1).takeWhile(_ != '{')).andThen(_.trim))

    lazy val recAndAlt = descriptions
      .filter(ln => ln.startsWith(RECOMMENDED) || ln.startsWith(ALTERNATIVE))
      .partition(_.startsWith(RECOMMENDED))
    (processLines(recAndAlt._1), processLines(recAndAlt._2))
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
