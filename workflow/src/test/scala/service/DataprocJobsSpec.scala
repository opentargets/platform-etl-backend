package service

import cats.effect.testing.scalatest.AsyncIOSpec
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import cats.syntax.option._
import io.opentargets.workflow.model.{Configuration, Job}
import io.opentargets.workflow.service.DataprocJobs
import org.scalatest.AppendedClues

class DataprocJobsSpec extends AsyncFreeSpec with Matchers with AsyncIOSpec with AppendedClues {
  "An OrderedJob should" - {
    "have dependent steps" in {
      // given
      val stepId = "arg"
      val job = Job(stepId, None, Seq("job1").some)
      // when
      val orderedJob = for {
        baseConf <- Configuration.load
      } yield DataprocJobs.createOrderedJob(job).run(baseConf.workflowResources)
      // /then
      orderedJob asserting (jobs =>
        jobs.getStepId shouldBe stepId
      ) withClue s"stepId on created job should be $stepId"
      orderedJob asserting (jobs =>
        jobs.getPrerequisiteStepIdsCount shouldBe 1 withClue "Prerequisite job count should be 1"
      )
    }
    "have no dependent steps" in {
      // given
      val job = Job("arg", None, None)
      val depCount = 0
      // when
      val orderedJob = for {
        conf <- Configuration.load
      } yield DataprocJobs.createOrderedJob(job).run(conf.workflowResources)
      // /then
      orderedJob asserting (jobs =>
        jobs.getPrerequisiteStepIdsCount shouldBe depCount withClue s"Prerequisite job count should be $depCount"
      )
    }
  }
  "Job filtering should" - {
    val jobs = Seq(
      Job("job1", None, Seq("job2").some),
      Job("job2", None, Seq("job3").some)
    )
    "remove dependencies not specified as a stageable job" in {
      // given
      val jobsToRun = List("job1")
      // when
      val results = DataprocJobs.filterJobs(jobsToRun, jobs)
      // then
      results.size shouldBe 1
    }
    "dependencies should be included when specified" in {
      // given
      val jobsToRun = List("job1", "job2")
      // when
      val results = DataprocJobs.filterJobs(jobsToRun, jobs)
      // then
      results.size shouldBe 2
    }
  }
  "The private workflow should" - {
    "have 6 steps" in {
      // given
      val expectedSteps =
        Seq("evidence", "associations", "search", "knownDrugs", "ebiSearch", "associationOTF")
      val expectedStepCount = expectedSteps.size
      val workflow = for {
        conf <- Configuration.load
      } yield DataprocJobs
        .createdOrderedJobs(conf.workflows.filter(_.name == "private").head)
        .run(conf)
      // then
      workflow asserting (wf => wf.size shouldBe expectedStepCount)
    }
  }
  "The public workflow should" - {
    "have all steps" in {
      // given
      val results = for {
        conf <- Configuration.load
      } yield (conf, DataprocJobs.createdOrderedJobs(conf.getDefaultWorkflow).run(conf))
      // when
      // then
      results asserting (r => {
        val (conf, wf) = r
        wf.size shouldBe conf.jobs.size
      })
    }
  }
}
