/**
  * Compare ES index counts
  *
  * Execute `curl --location --request GET 'http://localhost:9201/_cat/indices?v' > file.txt` to get file counts for new
  * and old ES instances. Then run this script.
  */

import scala.io.Source

val oldRelease = "2111.txt"
val newRelease = "2202.txt"

val indexes = (f: String) => Source.fromFile(f).getLines.toList.tail.map(_.split(" ").filter(_.nonEmpty))
  .map(ln => (ln(2), ln(6).toInt)).toMap


val o: Map[String, Int] = indexes(oldRelease)
val n: Map[String, Int] = indexes(newRelease)

// negative values mean fewer entries in this release than the last.
n.keys.map(k => (k, n(k) - o(k))).toMap

