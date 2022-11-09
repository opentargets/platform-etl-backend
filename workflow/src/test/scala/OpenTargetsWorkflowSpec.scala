import cats.effect.testing.scalatest.AsyncIOSpec
import io.opentargets.workflow.model.{Configuration, OpenTargetsWorkflow}
import org.scalatest.AppendedClues
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

class OpenTargetsWorkflowSpec
    extends AsyncFreeSpec
    with Matchers
    with AsyncIOSpec
    with AppendedClues {
  "getWorkflowArg should" - {
    "return the default workflow when no argument provide" in {
      // when
      val workflow =
        Configuration.load.map(conf => OpenTargetsWorkflow.getWorkflow("public", conf))
      // then
      workflow asserting (_.name shouldBe "public")
    }
    "return the default workflow when unknown argument provide" in {
      // when
      val workflow =
        Configuration.load.map(conf => OpenTargetsWorkflow.getWorkflow("opentargets", conf))
      // then
      workflow asserting (_.name shouldBe "public")
    }
    "return the selected workflow when valid (known workflow) argument provide" in {
      // when
      val workflow =
        Configuration.load.map(conf => OpenTargetsWorkflow.getWorkflow("private", conf))
      // then
      workflow asserting (_.name shouldBe "private")
    }
  }
}
