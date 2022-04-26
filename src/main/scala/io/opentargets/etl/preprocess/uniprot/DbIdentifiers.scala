package io.opentargets.etl.preprocess.uniprot

/** UniprotKB Database cross-references specified in the DR line.
  *
  * Each record contains zero or more database cross references.
  * The full list of available cross references is available
  * in the [[https://web.expasy.org/docs/userman.html#DR_line DR line documentation]].
  *
  * The records have the following format:
  *
  * {{{
  *   DR   RESOURCE_ABBREVIATION; RESOURCE_IDENTIFIER; OPTIONAL_INFORMATION_1[; OPTIONAL_INFORMATION_2][; OPTIONAL_INFORMATION_3].
  * }}}
  *
  * Supported databases are specified in this trait.
  */
trait DbIdentifiers {
  val CHEMBL = "ChEMBL;"
  val DRUGBANK = "DrugBank;"
  val PDB = "PDB;"
  val ENSEMBL = "Ensembl;"
  val GO = "GO;"
  val INTERPRO = "InterPro;"
  val REACTOME = "Reactome;"

  // separator between database name and identifier
  val POST_PROCESS_SPLIT = " "

  val dbOfInterest = Seq(
    CHEMBL,
    DRUGBANK,
    PDB,
    ENSEMBL,
    GO,
    INTERPRO,
    REACTOME
  )

  /** Returns line if it is a database we are interested in using.
    *
    * A database of interest is one that is specified in {{this.dbOfInterest}}
    */
  def isDbOfInterest(string: String): Boolean = {
    dbOfInterest.map(db => string.startsWith(db)).exists(b => b)
  }

  /** Return string of "[resource identifier] [resource abbreviation]" */
  def extractIdentifiers(string: String): String =
    string
      .split(POST_PROCESS_SPLIT)
      .take(2)
      .map(s => s.trim.dropRight(1)) // drop right as every entry ends with ';'
      .mkString(" ")
}
