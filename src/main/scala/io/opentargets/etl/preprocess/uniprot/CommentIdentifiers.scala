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

  case class UniprotFunctionsAndLocations(functions: Seq[String], locations: Seq[String])

  /** Group raw comments into proper comment entities.
    *
    * @param uniprotComments with array of 'raw' comments
    * @return UniprotFunctionsAndLocations contains parsed functions and locations.
    */
  def updateComments(uniprotComments: Iterator[String]): UniprotFunctionsAndLocations = {
    val newComments = concatenateComments(uniprotComments).filter(com =>
      COMMENTS_OF_INTEREST.exists(coi => coi.startsWith(com.takeWhile(_.isUpper))) && com.nonEmpty
    )
    partitionComments(newComments)
  }

  private def parseLocations(location: String): Seq[String] = {
    val isoformR = """\[Isoform [A-Z](\.)[0-9]\]:.+""" r
    val noNotes = location
      .split("Note=")

    val a: Array[String] = noNotes.headOption
      .map(opt => {
        val cleanedOfReferences = opt
          .replaceAll("\\{.+?\\}", "")
        // handle case where Uniprot record isn't standard, having form [Isoform A.1]:...
        val isoformStandardised: String = cleanedOfReferences match {
          case isoformR(_*) => cleanedOfReferences.replaceFirst("\\.", "-")
          case _            => cleanedOfReferences
        }
        isoformStandardised
          .split("\\.")
          .map(_.trim)
          .filter(!_.startsWith("Note="))
          .filter(_.nonEmpty)
      })
      .getOrElse(Array.empty)

    a
  }

  private def partitionComments(uniprotComments: Seq[String]): UniprotFunctionsAndLocations = {
    val (function, subcellularLocation) = uniprotComments.partition(_.startsWith("FUNCTION"))
    UniprotFunctionsAndLocations(
      cleanComments(function, FUNCTION),
      cleanComments(subcellularLocation, SUBCELL_LOCATION).flatMap(parseLocations)
    )
  }

  private def cleanComments(comments: Seq[String], commentType: String): Seq[String] = {
    val dropCount = (commentType + ": ").length
    comments.map(_.drop(dropCount))
  }

  @tailrec
  private def concatenateComments(
      comments: Iterator[String],
      buf: Seq[String] = Seq.empty,
      newComments: Seq[String] = Seq.empty
  ): Seq[String] = {
    // no more comments, return what we have
    if (comments.isEmpty) newComments :+ buf.mkString(" ")
    else {
      val head = comments.next
      // start of a new comment
      if (head.startsWith(NEW_COMMENT)) {
        // clear buffer and continue
        concatenateComments(
          comments,
          Seq(head.drop(NEW_COMMENT.length).trim),
          newComments :+ buf.mkString(" ")
        )
      } else {
        concatenateComments(comments, buf :+ head, newComments)
      }
    }
  }
}
