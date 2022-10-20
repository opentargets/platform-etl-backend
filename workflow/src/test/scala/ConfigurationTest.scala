import cats.effect.testing.scalatest.AsyncIOSpec
import model.Configuration
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

class ConfigurationTest extends AsyncFreeSpec with Matchers with AsyncIOSpec {
  "Configuration should" - {
    "be read from the resources directory" in {
      // when
      val conf = Configuration.load
      // then
      // if there is an error in reading the standard configuration this _does_ fail as the IO will contain an error.
      conf.asserting(_ => true shouldBe true)
    }
  }
}
