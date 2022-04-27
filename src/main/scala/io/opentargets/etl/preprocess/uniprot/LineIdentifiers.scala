package io.opentargets.etl.preprocess.uniprot

/** Specifies line identifiers used in UniportKB and used for parsing in this project. Not all Uniprot lines are supported.
  *
  * Details of specific lines and their meaning can be found in the [[https://web.expasy.org/docs/userman.html user guide]]
  * Uniprot uses line identifers to indicate which object field the subsequent string belongs to, as a single record
  * can span multiple lines.
  *
  * The following lines are available on entries:
  *
  * {{{
  *   Line code 	Content 	Occurrence in an entry
  *   ID	Identification	Once; starts the entry
  *   AC	Accession number(s)	Once or more
  *   DT	Date	Three times
  *   DE	Description	Once or more
  *   GN	Gene name(s)	Optional
  *   OS	Organism species	Once or more
  *   OG	Organelle	Optional
  *   OC	Organism classification	Once or more
  *   OX	Taxonomy cross-reference	Once
  *   OH	Organism host	Optional
  *   RN	Reference number	Once or more
  *   RP	Reference position	Once or more
  *   RC	Reference comment(s)	Optional
  *   RX	Reference cross-reference(s)	Optional
  *   RG	Reference group	Once or more (Optional if RA line)
  *   RA	Reference authors	Once or more (Optional if RG line)
  *   RT	Reference title	Optional
  *   RL	Reference location	Once or more
  *   CC	Comments or notes	Optional
  *   DR	Database cross-references	Optional
  *   PE	Protein existence	Once
  *   KW	Keywords	Optional
  *   FT	Feature table data	Once or more in Swiss-Prot, optional in TrEMBL
  *   SQ	Sequence header	Once
  *   (blanks)	Sequence data	Once or more
  *   //	Termination line	Once; ends the entry
  * }}}
  */
trait LineIdentifiers {
  /* UniprotKb manual: https://www.uniprot.org/help/uniprotkb_manual Entry record guide:
   *  */
  val END_RECORD = "//"
  val ID = "ID" // once
  val ACCESSION = "AC" // once or more
  val DESCRIPTION = "DE" // once or more
  val DATABASE_XREF = "DR" // optional
  val COMMENT = "CC" // optional
  val GENE_SYMBOLS = "GN" // symbol synonyms
  lazy val identifiers: List[String] =
    ID :: ACCESSION :: DESCRIPTION :: DATABASE_XREF :: COMMENT :: GENE_SYMBOLS :: Nil
}
