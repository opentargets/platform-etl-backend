package io.opentargets.workflow.service

import cats.effect.IO
import cats.implicits.toTraverseOps
import io.opentargets.workflow.model.OpenTargetsWorkflow.ResourcesToMove
import org.typelevel.log4cats.slf4j.loggerFactoryforSync
import org.typelevel.log4cats.{LoggerFactory, SelfAwareStructuredLogger}

import scala.language.postfixOps
import sys.process._

/** Utility object to move resources within GCP. Requires that gsutil is installed on the machine
  * running the program. The current Java API for interacting with Google storage does not allow the
  * easy transfer of 'directories'. Saving us the trouble of recursing and moving we can use the
  * system utilities to do it quickly.
  */
object ResourceTransfer {

  type ErrorOr[T] = Either[Exception, T]
  val logger: SelfAwareStructuredLogger[IO] = LoggerFactory[IO].getLogger

  val requiredUtils: Seq[String] = Seq("gsutil")

  private def validateUtilsInstalled(required: Seq[String] = requiredUtils): ErrorOr[Boolean] =
    Either.cond(
      allUtilsInstalled(required),
      true,
      new Exception(s"Required utility not installed. Requires $required")
    )

  /** @param utils
    *   that must be installed
    * @return
    *   true if all programs in `utils` are on the unix PATH.
    */
  private def allUtilsInstalled(utils: Seq[String]): Boolean = {
    val cmd = utils.mkString("which ", " ", "")
    logger.error(s"Command to execute: $cmd")
    (cmd !) == 0
  }

  /** @param from
    *   path to copy from
    * @param to
    *   path to copy to
    * @return
    *   exit code of gsutil copy command.
    */
  private def copyFromTo(from: String, to: String): IO[Int] =
    IO(s"gsutil -m cp -r $from $to" !).flatTap {
      case 0     => logger.debug(s"Move $from to $to: Exit code 0.")
      case _ @ec => logger.error(s"Move $from to $to: Exit code $ec.")
    }

  def moveResources(resourcesToMove: ResourcesToMove): IO[List[(String, Int)]] =
    if (resourcesToMove.isEmpty) {
      logger.info("No resources to move") >> IO(List.empty)
    } else
      resourcesToMove
        .map(resource =>
          copyFromTo(resource._1, resource._2).map(exitCode => (resource._1, exitCode))
        )
        .sequence

  def execute(resource: ResourcesToMove): IO[Int] = {
    val isSystemConfigured = validateUtilsInstalled()
    isSystemConfigured match {
      case Left(value) => IO.raiseError(value)
      case Right(_) =>
        val rs = for {
          r <- moveResources(resource).map(ls => ls.filter(_._2 != 0))
        } yield r
        rs.map(ls => if (ls.nonEmpty) 0 else 1)
    }
  }
}
