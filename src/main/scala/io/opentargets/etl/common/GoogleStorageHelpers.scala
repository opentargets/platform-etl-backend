package io.opentargets.etl.common;

object GoogleStorageHelpers {

  /** @path path to google storage blob.
    * @return a tuple of (bucket, blob) for a given path.
    */
  def pathToBucketBlob(path: String): (String, String) = {
    require(path.trim.startsWith("gs://"))
    val noPrefix = path.drop(5)
    val bucketAndBlob = noPrefix.splitAt(noPrefix.prefixLength(_ != '/'))
    (
      bucketAndBlob._1,
      bucketAndBlob._2.drop(1) + "/"
    ) // remove leading '/', add tailing '/' so we get the output dir.
  }

  def isGoogleStoragePath(path: String): Boolean = path.startsWith("gs://")

}
