package io.opentargets.etl.preprocess.uniprot

import better.files.File
import com.typesafe.scalalogging.LazyLogging
import org.json4s.DefaultFormats

import scala.annotation.tailrec

/** Final structure of preprocessed Uniprot data file */
case class UniprotEntryParsed(
    id: String,
    accessions: Seq[String],
    names: Seq[String],
    synonyms: Seq[String],
    dbXrefs: Seq[String],
    functions: Seq[String],
    locations: Seq[String]
)

/** Intermediate structure of preprocessed Uniprot.
  *
  * This should not be used outside this package.
  */
case class UniprotEntry(
    id: String = "",
    accession: Seq[String] = Seq.empty,
    description: Seq[String] = Seq.empty,
    dbXrefs: Seq[String] = Seq.empty,
    comments: Seq[String] = Seq.empty,
    functions: Seq[String] = Seq.empty,
    locations: Seq[String] = Seq.empty
) {
  def convertToParsed(): UniprotEntryParsed = {
    val (name, syns) = UniprotConverter.processNames(description)
    UniprotEntryParsed(id, accession, name, syns, dbXrefs, functions, locations)
  }
}

/**
  * Converts between Uniprot flat data file and UniprotEntryParsed
  */
object UniprotConverter
    extends LineIdentifiers
    with DbIdentifiers
    with CommentIdentifiers
    with DescriptionIdentifiers
    with LazyLogging {

  /** Returns list of UniprotEntryParsed based on provided iterator of UniprotKB data from flat file format.
    *
    * Uniprot provides their knowledge base in a flat file format specified in the
    * [[https://www.uniprot.org/help/uniprotkb_manual UniprotKb manual]]. A more detailed guide for each record can be
    * found in the [[ https://web.expasy.org/docs/userman.html user guide]].
    *
    * There is a trait for each of the line types of interest (CC - comment, DE - description, etc) with methods to
    * parse meaningful results from those lines.
    *
    * The input file is provided from the [[https://github.com/opentargets/platform-input-support platform-input-support]]
    * project.
    *
    * The caller of the class is responsible for managing the incoming file and ensuring that it is properly closed.
    *
    * @param data from UniprotKB flat file.
    * @return
    */
  def convertUniprotFlatFileToUniprotEntry(data: Iterator[String]): List[UniprotEntryParsed] = {
    @tailrec
    def go(input: Iterator[String],
           computed: List[UniprotEntryParsed] = List()): List[UniprotEntryParsed] = {
      if (input.isEmpty) computed
      else {
        // get entry
        val singleEntry = input.takeWhile(!_.startsWith(END_RECORD))

        // turn into case class
        val uniprotEntry: UniprotEntry = convertUniprotEntry(singleEntry)

        val uniprotEntryWithUpdatedComments: UniprotEntry = updateComments(uniprotEntry)
        go(input, uniprotEntryWithUpdatedComments.convertToParsed :: computed)
      }
    }

    go(data)
  }

  private def convertUniprotEntry(entry: Iterator[String]): UniprotEntry = {
    @tailrec
    def go(inputs: Iterator[String], uniprotEntry: UniprotEntry): UniprotEntry = {
      if (inputs.isEmpty) uniprotEntry
      else {
        inputs.next match {
          case i if i.startsWith(ID) =>
            go(inputs, uniprotEntry.copy(id = processId(i)))
          case ac if ac.startsWith(ACCESSION) =>
            val acs = removeLineIndex(ac).split(";")
            go(inputs, uniprotEntry.copy(accession = uniprotEntry.accession ++ acs))
          case desc if desc.startsWith(DESCRIPTION) =>
            go(inputs,
               uniprotEntry.copy(description = uniprotEntry.description :+ removeLineIndex(desc)))
          case dbRef if dbRef.startsWith(DATABASE_XREF) =>
            val dbs = removeLineIndex(dbRef)
            if (isDbOfInterest(dbs))
              go(inputs,
                 uniprotEntry.copy(dbXrefs = uniprotEntry.dbXrefs :+ extractIdentifiers(dbs)))
            else go(inputs, uniprotEntry)
          case comment if comment.startsWith(COMMENT) =>
            val line = removeLineIndex(comment)
            // The last comment is followed by a copywrite notice which starts with a line of dashes. We want to ignore
            // everything after that, so if we encounter it continue.
            if (line.startsWith("----")) go(inputs.drop(3), uniprotEntry)
            else
              go(inputs, uniprotEntry.copy(comments = uniprotEntry.comments :+ line))
          case _ => go(inputs, uniprotEntry)
        }
      }
    }

    go(entry, UniprotEntry())
  }

  /**
    * All lines from the flat document Uniprot file have the form "XX blahblah: boohoo" where 'XX' indicates the section.
    * After we have used this information for filtering we want to discard it.
    *
    * @param string raw input line from Uniprot
    * @return input line with first two characters and white-space removed.
    */
  private def removeLineIndex(string: String): String = string.drop(2).stripLeading

  private def processId(idLine: String): String =
    removeLineIndex(idLine).takeWhile(chr => !chr.isWhitespace)

}

/**
  * Convert from flat Uniprot file to JSON and save results.
  *
  * Requires two program arguments: input file, output directory.
  *
  * To use: java -cp io-opentargets-etl-backend-assembly-[version].jar io.opentargets.etl.preprocess.uniprot \
  * input file \
  * output file
  *
  * Note: jar file must have been assembled with Spark bundled as it relies on the JSON library bundled with Spark.
  */
object Main extends App with LazyLogging {
  assert(
    args.length == 2,
    """Two arguments required for program, input file of Uniprot text entries and
      |output file destination to save results in JSON format.""".stripMargin
  )
  val inputPath = args.head
  assert(inputPath.endsWith("txt") || inputPath.endsWith("txt.gz"),
         "Input file must be a text file.")

  import org.json4s.jackson.Serialization.write

  implicit val jsonFormat: DefaultFormats.type = DefaultFormats

  logger.info("Preparing to convert Uniprot raw text file to json.")
  logger.info(s"""Input: ${inputPath}
                 |Ouput: ${args.tail.head}""".stripMargin)

  val inputLines: Iterator[String] = File(inputPath).lineIterator
  val outputFile: File = File(args.tail.head).createFileIfNotExists(createParents = true)
  val entries = UniprotConverter.convertUniprotFlatFileToUniprotEntry(inputLines)

  entries.foreach(e => outputFile.appendLine(write(e)))
  logger.info("Completed preprocessing of Uniprot flat file.")

}
