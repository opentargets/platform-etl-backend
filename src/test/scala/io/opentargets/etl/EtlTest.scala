package io.opentargets.etl

import io.opentargets.etl.backend.ETLSessionContext
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class EtlTest extends AnyFlatSpec with MockFactory {
  "The pipeline" should "fail given an unknown step" in {
    val esc: ETLSessionContext = mock[ETLSessionContext]
    assertThrows[IllegalArgumentException](ETL.applySingleStep("foo")(esc))
  }
}
