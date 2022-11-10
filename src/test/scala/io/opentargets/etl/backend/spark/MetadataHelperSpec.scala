package io.opentargets.etl.backend.spark

import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

class MetadataHelperSpec
    extends AnyFlatSpec
    with Matchers
    with TableDrivenPropertyChecks
    with PrivateMethodTester {
  "Content root" should "be extracted from filepath" in {
    val func = PrivateMethod[String]('getContentPath)
    val tests = Table(
      ("input", "format", "expected"),
      ("gs://open-targets-pre-data-releases/22.11/output/etl/parquet/reactome",
       "parquet",
       "/reactome"
      ),
      ("gs://open-targets-pre-data-releases/22.11/output/etl/parquet/fda/adverseTargetReactions",
       "parquet",
       "/fda/adverseTargetReactions"
      ),
      ("gs://open-targets-pre-data-releases/22.11/output/etl/json/fda/adverseTargetReactions",
       "json",
       "/fda/adverseTargetReactions"
      )
    )
    forAll(tests) { (in: String, fmt: String, exp: String) =>
      MetadataHelper invokePrivate func(in, fmt) should be(exp)
    }
  }
}
