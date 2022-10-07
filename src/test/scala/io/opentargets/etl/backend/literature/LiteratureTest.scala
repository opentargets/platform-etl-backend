package io.opentargets.etl.backend.literature

import io.opentargets.etl.backend.EtlSparkUnitTest
import org.scalatest.matchers.should.Matchers._
import org.scalatest.Inspectors._

class LiteratureTest extends EtlSparkUnitTest {

  "Literature class" should "create a new session with lit specific configurations" in {
    import sparkSession.implicits._
    implicit val context = ctx
    val session = Literature.createETLSession()

    session should not be (null)

    val sessionConfigurations = session.sparkSession.conf.getAll
    val configurations = session.configuration.literature.common.sparkSessionConfig.get
      .map((conf) => (conf.k, conf.v))
      .toMap
      .keys

    forAll(configurations)(conf => sessionConfigurations should contain key (conf))

  }

}
