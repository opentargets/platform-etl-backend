package io.opentargets.etl.common

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.must.Matchers

class GoogleStorageHelpersSpec extends AnyFlatSpecLike with Matchers with LazyLogging {
  val gsPath = "gs://bucket/blob/blobbly/blobblier"

  "A path" should "be correctly identified as belonging to GCP Storage" in {
    assert(GoogleStorageHelpers.isGoogleStoragePath(gsPath), "Should be true, but was false!")
  }
  it should "recognise when a path is not a GCP storage object" in {
    val nonGsPath = "/home/user/path/to/data"
    assertResult(false)(GoogleStorageHelpers.isGoogleStoragePath(nonGsPath))
  }
  "A bucket and blob" should "be extracted from a google storage path" in {
    val (bucket, blob) = GoogleStorageHelpers.pathToBucketBlob(gsPath)
    assertResult("bucket")(bucket)
    assertResult("blob/blobbly/blobblier/")(blob)
  }
}
