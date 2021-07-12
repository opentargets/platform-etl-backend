package io.opentargets.etl.preprocess.go

import scala.annotation.tailrec
import scala.collection.mutable

case class Go(id: String, name: String)

object GoConverter {

  private val startEntry = (line: String) => line.startsWith("[Term]")
  private val endEntry = (line: String) => line.isEmpty
  private val excludedEntry = (line: String) => line.startsWith("[Typedef]")
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
    def getSingleEntry(acc: Seq[String] = Seq.empty[String]): Seq[String] = {
      if (file.hasNext) {
        val line = file.next()
        if (endEntry(line)) acc
        else if (excludedEntry(line)) {
          scrollToNext()
          getSingleEntry()
        } else if (startEntry(line)) getSingleEntry(acc)
        else getSingleEntry(line +: acc)
      } else acc
    }

    @tailrec
    def go(acc: Seq[Go] = List.empty): Seq[Go] = {
      val entry = getSingleEntry()
      if (entry.nonEmpty) {
        val map: mutable.Map[String, String] = scala.collection.mutable.Map()
        for (line <- entry) {
          val kv = line.split(separator, 2)
          val key = kv.head
          if (fields.contains(key)) map(key) = kv.tail.head
        }
        val goEntry = Go(map("id").trim, map("name").trim)

        if (file.hasNext) go(goEntry +: acc) else goEntry +: acc
      } else acc

    }

    // scroll to start
    @tailrec
    def scrollToNext(): Unit = {
      if (file.hasNext && !startEntry(file.next())) scrollToNext()
    }

    scrollToNext()
    go()
  }
}
