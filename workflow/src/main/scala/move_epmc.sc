import cats.effect.{ExitCode, IO, IOApp, Resource}
import cats.effect.unsafe.implicits.global

import scala.io.{BufferedSource, Source}
import scala.util.matching.Regex

// read files

object App extends IOApp {
  val file = "/home/jarrod/development/platform-etl-backend/data/epmc_files.txt"
  val gsutil = "/home/jarrod/programs/google-cloud-sdk/bin/gsutil"
  def getSource(f: String): Resource[IO, BufferedSource] =
    Resource.make {
      IO.blocking(Source.fromFile(f)) // build
    } { resource =>
      IO.unit
//      IO.blocking(resource.close())
//        .handleErrorWith(err => IO(println(err.getMessage)) >> IO.unit) // release
    }

  val dateRegexStr = "[0-9]{2}_[0-9]{2}_[0-9]{4}"
  def getDateFromFilename(line: String): Option[String] = {
    val dateR: Regex = dateRegexStr.r
    dateR.findFirstIn(line)
  }
  def convertDateFromDMYtoYMD(date: String): String = {
    require(date.matches(dateRegexStr))
    val dateParts = date.split('_')
    require(dateParts.length == 3, "Date should have three parts")
    s"${dateParts(2)}_${dateParts(1)}_${dateParts(0)}"
  }

// get old date
  def linesIteratorResource: IO[Iterator[String]] = getSource(file).use(s => IO(s.getLines()))
  type LineAndDate = Option[(String, String, String)]
  def lineToDate(line: String): LineAndDate = {
    val oldDate: Option[String] = getDateFromFilename(line)
    val newdate: Option[String] = oldDate.map(convertDateFromDMYtoYMD)
    val dates = for {
      o <- oldDate
      n <- newdate
    } yield (line, o, n)
    dates
  }
  def linesToDates(lines: Iterator[String]): Iterator[LineAndDate] = lines.map(lineToDate)
  def datesToFilenames(in: LineAndDate): Option[(String, String)] = in map { o =>
    val (file, old, newD) = o
    (file, file.replace(old, newD))
  }

  def makeGsutilMove(fromTo: (String, String)): String =
    s"$gsutil -m cp -r ${fromTo._1} ${fromTo._2}"

  val commands: IO[List[String]] = for {
    lines <- linesIteratorResource
  } yield {
    val linesAndDate: Iterator[LineAndDate] = linesToDates(lines)
    val oldAndNewFile = linesAndDate.map(datesToFilenames)
    oldAndNewFile.flatMap(files => files.map(makeGsutilMove)).toList
  }

  import scala.sys.process._
  def executeCommands(cmds: List[String]): List[(Int, String)] = cmds.map(cmd => (cmd.!, cmd))
  def run(args: List[String]) = for {
    cmd <- commands
    resultCodes <- IO(executeCommands(cmd))
    _ <- IO {
      println("Unsuccessful operations:")
      resultCodes.filter(_._1 != 0).foreach(println)
    }
//    _ <- IO(cmd.foreach(println))

  } yield ExitCode.Success
}
App.run(List.empty).unsafeRunSync()
/*
gsutil mv <old> <new>
gsuilt -m mv -r gs://otar025-epmc/Abstracts/20_07_2022/ gs://otar025-epmc/Abstracts/2022_07_20/
 */

//val testStr = "gs://otar025-epmc/Abstracts/27_08_2022/"
//App.getDateFromFilename(testStr)
