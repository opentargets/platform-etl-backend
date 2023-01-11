import scala.io.Source
import scala.sys.process._
val file = "/home/jarrod/programs/lit_files.txt"
val src = Source.fromFile(file)
val iter = src.getLines.toList
src.close()
val update = (str: String) => str.replaceFirst("[0-9]{2}_[0-9]{2}_[0-9]{4}/", "")
val fromTo = iter.map(it => (it, update(it)))
val commands = fromTo.map(it => {
  val (f, t) = it
  s"/home/jarrod/programs/google-cloud-sdk/bin/gsutil -m mv $f $t"
})
lazy val execute = commands.map(it => (it, it.!)).filter(_._2 != 0)

val results =
  s"""
    |Files to transfer: ${commands.size}
    |Success: ${commands.size - execute.size}
    |Failure: ${execute.size}
    |${ if (execute.nonEmpty) execute.toString}
    |
    |""".stripMargin

print(results)
