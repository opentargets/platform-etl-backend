package service

import cats.effect.testing.scalatest.AsyncIOSpec
import model.{Configuration, Job}
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import cats.syntax.option._
import org.scalatest.AppendedClues

class DataprocJobsSpec extends AsyncFreeSpec with Matchers with AsyncIOSpec with AppendedClues {
  "An OrderedJob should" - {
    "have dependent steps" in {
      // given
      val stepId = "arg"
      val job = Seq(Job(stepId, None, Seq("job1").some))
      // when
      val orderedJob = for {
        baseConf <- Configuration.load
      } yield DataprocJobs.createdOrderedJobs.run(baseConf.copy(jobs = job))
      // /then
      orderedJob asserting (jobs => jobs.size shouldBe 1) withClue "There should be one job"
      orderedJob asserting (jobs =>
        jobs.head.getStepId shouldBe stepId
      ) withClue s"stepId on created job should be $stepId"
      orderedJob asserting (jobs =>
        jobs.head.getPrerequisiteStepIdsCount shouldBe 1 withClue "Prerequisite job count should be 1"
      )
    }
    "have no dependent steps" in {
      // given
      val job = Seq(Job("arg", None, None))
      val depCount = 0
      // when
      val orderedJob = for {
        baseConf <- Configuration.load
      } yield DataprocJobs.createdOrderedJobs.run(baseConf.copy(jobs = job))
      // /then
      orderedJob asserting (jobs => jobs.size shouldBe 1)
      orderedJob asserting (jobs =>
        jobs.head.getPrerequisiteStepIdsCount shouldBe depCount withClue s"Prerequisite job count should be $depCount"
      )
    }
  }
}
