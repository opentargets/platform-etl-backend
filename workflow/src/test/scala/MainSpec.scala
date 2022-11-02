import cats.effect.testing.scalatest.AsyncIOSpec
import model.Configuration
import org.scalatest.AppendedClues
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

class MainSpec extends AsyncFreeSpec with Matchers with AsyncIOSpec with AppendedClues {
  "getWorkflowArg should" - {
    "return the default workflow when no argument provide" in {
      // when
      val workflow = Configuration.load.map(conf => Main.getWorkflowArg(List.empty, conf))
      // then
      workflow asserting (_ shouldBe Main.defaultWorkflow)
    }
    "return the default workflow when unknown argument provide" in {
      // when
      val workflow = Configuration.load.map(conf => Main.getWorkflowArg(List("opentargets"), conf))
      // then
      workflow asserting (_ shouldBe Main.defaultWorkflow)
    }
    "return the selected workflow when valid (known workflow) argument provide" in {
      // when
      val workflow = Configuration.load.map(conf => Main.getWorkflowArg(List("private"), conf))
      // then
      workflow asserting (_ shouldBe "private")
    }
  }
}
