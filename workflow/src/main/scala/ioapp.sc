import cats.effect.unsafe.implicits.global
import cats.effect.{ExitCode, IO, IOApp}
import com.google.cloud.storage.{BlobId, CopyWriter, Storage, StorageOptions}

object Worksheet extends IOApp {
  def copyBlob(storage: Storage, from: String, to: String): CopyWriter = {
    val (fromBucket, fromBlob) = pathToBucketBlob(from)
    val (toBucket, toBlob) = pathToBucketBlob(to)
    val source = BlobId.of(fromBucket, fromBlob)
    val target = BlobId.of(toBucket, toBlob)
    val copyRequest = Storage.CopyRequest.newBuilder.setSource(source).setTarget(target).build()
    storage.copy(copyRequest)
  }

  private def pathToBucketBlob(path: String): (String, String) = {
    require(path.trim.startsWith("gs://"))

    val noPrefix = path.drop(5)
    val bucketAndBlob = noPrefix.splitAt(noPrefix.prefixLength(_ != '/'))
    (
      bucketAndBlob._1,
      bucketAndBlob._2.drop(1) + "/"
    ) // remove leading '/', add tailing '/' so we get the output dir.

  }
  val storage = StorageOptions.newBuilder.setProjectId("open-targets-eu-dev").build.getService

  def run(args: List[String]): IO[ExitCode] =
    for {
      _ <- IO(println("Starting copy"))
      cw <- IO(copyBlob(storage, "gs://ot-team/jarrod/uniprot", "gs://ot-team/asier/uniprot"))
      _ <- IO(println(s"Copied ${cw.getTotalBytesCopied} bytes"))
    } yield ExitCode.Success
}

Worksheet.run(List.empty).unsafeRunSync()
