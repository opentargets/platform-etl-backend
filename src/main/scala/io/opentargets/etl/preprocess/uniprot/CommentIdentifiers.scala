package io.opentargets.etl.preprocess.uniprot

import scala.annotation.tailrec

/** Comments hold a miscallaneous range of data fields on the protein.
  *
  * The format of a comment block is:
  *
  * {{{
  * CC   -!- TOPIC: First line of a comment block;
  * CC       second and subsequent lines of a comment block.
  * }}}
  *
  * Available fields:
  *
  *   - ALLERGEN
  *   - ALTERNATIVE PRODUCTS
  *   - BIOPHYSICOCHEMICAL PROPERTIES
  *   - BIOTECHNOLOGY 	
  *   - CATALYTIC ACTIVITY
  *   - CAUTION 	
  *   - COFACTOR
  *   - DEVELOPMENTAL STAGE 	
  *   - DISEASE 	
  *   - DISRUPTION PHENOTYPE 	
  *   - DOMAIN 	
  *   - ACTIVITY REGULATION 	
  *   - FUNCTION 	
  *   - INDUCTION 	
  *   - INTERACTION
  *   - MASS SPECTROMETRY
  *   - MISCELLANEOUS 	
  *   - PATHWAY 	
  *   - PHARMACEUTICAL 	
  *   - POLYMORPHISM 	
  *   - PTM 	
  *   - RNA EDITING 	
  *   - SEQUENCE CAUTION 	
  *   - SIMILARITY 	
  *   - SUBCELLULAR LOCATION 	
  *   - SUBUNIT 	
  *   - TISSUE SPECIFICITY 	
  *   - TOXIC DOSE 	
  *   - WEB RESOURCE 	
  */
trait CommentIdentifiers {

  val NEW_COMMENT = "-!-"
  val SUBCELL_LOCATION = "SUBCELLULAR LOCATION"
  val FUNCTION = "FUNCTION"
  val COMMENTS_OF_INTEREST = Seq(SUBCELL_LOCATION, FUNCTION)

  /**
    * Group raw comments into proper comment entities.
    *
    * @param uniprotEntry with array of 'raw' comments
    * @return uniprotEntry with only the comments of interest and each of those concatenated into a string.
    */
  def updateComments(uniprotEntry: UniprotEntry): UniprotEntry = {
    val comments = uniprotEntry.comments.iterator
    val newComments = concatenateComments(comments).filter(com =>
      COMMENTS_OF_INTEREST.exists(coi => coi.startsWith(com.takeWhile(_.isUpper))) && com.nonEmpty)
    partitionComments(uniprotEntry.copy(comments = newComments))

  }

  private def partitionComments(uniprotEntry: UniprotEntry): UniprotEntry = {
    val (function, subcellularLocation) = uniprotEntry.comments.partition(_.startsWith("FUNCTION"))
    uniprotEntry.copy(functions = cleanComments(function, FUNCTION),
                      locations = cleanComments(subcellularLocation, SUBCELL_LOCATION))
  }

  private def cleanComments(comments: Seq[String], commentType: String): Seq[String] = {
    val dropCount = (commentType + ": ").length
    comments.map(_.drop(dropCount))
  }

  @tailrec
  private def concatenateComments(comments: Iterator[String],
                                  buf: Seq[String] = Seq.empty,
                                  newComments: Seq[String] = Seq.empty): Seq[String] = {
    // no more comments, return what we have
    if (comments.isEmpty) newComments :+ buf.mkString(" ")
    else {
      val head = comments.next
      // start of a new comment
      if (head.startsWith(NEW_COMMENT)) {
        // clear buffer and continue
        concatenateComments(comments,
                            Seq(head.drop(NEW_COMMENT.length).stripLeading),
                            newComments :+ buf.mkString(" "))
      } else {
        concatenateComments(comments, buf :+ head, newComments)
      }
    }
  }
}
