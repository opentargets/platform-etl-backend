package io.opentargets.etl.preprocess.go

import scala.annotation.tailrec
import scala.collection.mutable

case class Go(id: String, name: String)

object GoConverter {
  private val startEntry = (line: String) => line.trim.startsWith("[Term]")
  private val endEntry = (line: String) => line.trim.nonEmpty
  private val separator = ":"

  /**
    *
    * @param file input file in obo format available from [[http://geneontology.org/docs/download-ontology/#go_obo_and_owl here]]
    *             The file is separated into entries which start with [Term] and end with a blank line.
    *
    *             There are a large number of fields available as specified in the [[http://owlcollab.github.io/oboformat/doc/obo-syntax documentation]].
    *             Fields are key-value pairs separated by a colon.
    *
    *             This method only extracts the id and name field.
    * @return
    */
  def convertFileToGo(file: Iterator[String], fields: Set[String] = Set("id", "name")): Seq[Go] = {
    @tailrec
    def go(lines: Iterator[String], entries: Seq[Go]): Seq[Go] = {
      lines.hasNext match {
        case true =>
          val entryIt = lines.dropWhile(startEntry)
          val vec = entryIt
            .drop(1)
            .takeWhile(endEntry)
            .withFilter(el => fields.exists(el.startsWith))
            .flatMap(_.split(separator).drop(1).map(_.trim))
            .toList
          vec match {
            case "GO" :: id :: name :: Nil => go(lines, entries :+ Go("GO:" + id, name))
            case _                         => go(lines, entries)
          }
        case false => entries
      }
    }

    go(file, Seq.empty[Go])
  }
}
