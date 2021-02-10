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
    val recAndAlt = descriptions
      .filter(ln => ln.startsWith(RECOMMENDED) || ln.startsWith(ALTERNATIVE))
      .partition(_.startsWith(RECOMMENDED))
    val recommended = recAndAlt._1.map(removeLeadingMetadata.andThen(_.dropRight(1)))
    val alternative = recAndAlt._2.map(removeLeadingMetadata.andThen(_.dropRight(1)))
    (recommended, alternative)
  }
}
