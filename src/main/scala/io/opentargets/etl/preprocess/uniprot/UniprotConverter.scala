package io.opentargets.etl.preprocess.uniprot

import better.files.File
import com.typesafe.scalalogging.LazyLogging
import org.json4s.DefaultFormats
import io.opentargets.etl.common.ConsumeWhileIterator._

/** Final structure of preprocessed Uniprot data file */
case class UniprotEntry(
    id: String,
    accessions: Seq[String],
    names: Seq[String],
    synonyms: Seq[String],
    symbolSynonyms: Seq[String],
    dbXrefs: Seq[String],
    functions: Seq[String],
    locations: Seq[String]
)

/** Converts between Uniprot flat data file and UniprotEntryParsed
  */
object UniprotConverter
    extends LineIdentifiers
    with DbIdentifiers
    with CommentIdentifiers
    with DescriptionIdentifiers
    with GeneIdentifiers
    with LazyLogging {

  /** Returns list of UniprotEntry based on provided iterator of UniprotKB data from flat file format.
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
    * @return a seq of UniprotEntry elements
    */
  def fromFlatFile(data: Iterator[String]): Seq[UniprotEntry] = {
    data
      .consumeWhile(_.takeWhile(!_.startsWith(END_RECORD)))(convertUniprotEntry)
  }

  private def convertUniprotEntry(entry: Seq[String]): UniprotEntry = {
    val mappedLines = entry
      .groupBy(_.takeWhile(!_.isSpaceChar))
      .filterKeys(identifiers.contains)
      .mapValues(f =>
        f.map(
          _.dropWhile(!_.isSpaceChar)
            .replaceAll("\\{.*\\}", "")
            .trim
        )
      )
      .withDefaultValue(Stream.empty[String])

    val id = processId(mappedLines(ID))
    val accessions = mappedLines(ACCESSION).flatMap(_.split(";"))
    val descriptions = mappedLines(DESCRIPTION)
    val dbXrefs =
      mappedLines(DATABASE_XREF).withFilter(isDbOfInterest).map(extractIdentifiers)
    val comments = mappedLines(COMMENT).takeWhile(!_.startsWith("----"))
    val geneSymbols = mappedLines(GENE_SYMBOLS)
    val funcAndLocs = updateComments(comments.toIterator)

    val names = processNames(descriptions)
    val symbolSynonyms = processSymbolSynonyms(geneSymbols)

    UniprotEntry(
      id,
      accessions,
      names.recNames,
      names.altNames,
      symbolSynonyms ++ names.symbols,
      dbXrefs,
      funcAndLocs.functions,
      funcAndLocs.locations
    )
  }

  private def processId(lines: Seq[String]): String =
    lines.headOption.getOrElse("").takeWhile(!_.isSpaceChar)
}

/** Convert from flat Uniprot file to JSON and save results.
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
  assert(
    inputPath.endsWith("txt") || inputPath.endsWith("txt.gz"),
    "Input file must be a text file."
  )

  import org.json4s.jackson.Serialization.write

  implicit val jsonFormat: DefaultFormats.type = DefaultFormats

  logger.info("Preparing to convert Uniprot raw text file to json.")
  logger.info(s"""Input: $inputPath
                 |Ouput: ${args.tail.head}""".stripMargin)

  val inputLines: Iterator[String] = File(inputPath).lineIterator
  val outputFile: File = File(args.tail.head).createFileIfNotExists(createParents = true)
  val entries = UniprotConverter.fromFlatFile(inputLines)

  entries.foreach(e => outputFile.appendLine(write(e)))
  logger.info("Completed preprocessing of Uniprot flat file.")

}
