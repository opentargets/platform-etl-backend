import ammonite.ops._

val ouputs = ls! wd

val metadata = ls.rec! wd |? (_.ext == "json")

metadata.foreach( f => {
  println(s"Columns for: ${f.getSegment(f.segmentCount - 2 )}")
  %('jq, ".columns", f)
})