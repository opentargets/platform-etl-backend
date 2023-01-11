//https://cloud.google.com/storage/docs/reference/libraries#client-libraries-install-java
//import $ivy.`com.google.cloud:google-cloud-storage:2.8.0`

import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import com.google.api.gax.paging.Page
import com.google.cloud.storage.Blob
import com.google.cloud.storage.Storage.BlobListOption
import io.opentargets.etl.common.GoogleStorageHelpers
import io.opentargets.etl.common.GoogleStorageHelpers.pathToBucketBlob

import scala.collection.JavaConverters

val storage: Storage = StorageOptions.getDefaultInstance.getService

val bucketName = "ot-team"
val dir = "/jarrod"

val outputDir = "gs://open-targets-pre-data-releases/22.02.1/output/etl/parquet/"

val (bucket, blob) = GoogleStorageHelpers.pathToBucketBlob(outputDir)

val blobs: Page[Blob] =
  storage.list(bucket, BlobListOption.currentDirectory(), BlobListOption.prefix(blob))

val b: Map[String, Boolean] = JavaConverters
  .asScalaIteratorConverter(blobs.iterateAll().iterator())
  .asScala
  .filter(_.isDirectory)
  .map(_.getName -> true)
  .toMap
blobs.iterateAll().forEach(blob => println(s"${blob.getName} is directory ${blob.isDirectory}"))

storage.list().iterateAll().forEach(bucket => println(bucket.getName))

// Genetics - check if all directories expected are present
val expectedDirs =
  Set("d2v2g",
      "d2v2g_scored",
      "l2g",
      "lut",
      "manhattan",
      "sa",
      "v2d",
      "v2d_coloc",
      "v2d_credset",
      "v2g",
      "v2g_scored",
      "variant-index")
val storage: Storage = StorageOptions.getDefaultInstance.getService

val outputDir = "gs://genetics-portal-dev-data/22.01.2/outputs/"

val (bucket, blob) = pathToBucketBlob(outputDir)

val blobs: Page[Blob] =
    storage.list(bucket, BlobListOption.currentDirectory(), BlobListOption.prefix(blob))

val blobsFound: Set[String] = JavaConverters
  .asScalaIteratorConverter(blobs.iterateAll().iterator())
  .asScala
  .filter(_.isDirectory)
  .map(_.getName)
  .toSet

val pattern = "([^\\/]+)\\/?$".r
val directoriesFound = blobsFound.flatMap(pattern.findFirstIn(_)).map(_.dropRight(1))

val missing = expectedDirs diff directoriesFound
println(s"Missing directories: $missing")
