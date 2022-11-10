package io.opentargets.etl.backend.spark

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.spark.MetadataHelper.getContentPath
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.current_timestamp

trait MetadataWriter {
  def create(ioResource: IOResource): IOResource
  def write(ioResource: IOResource): Unit
}

object MetadataHelper {

  /** For the metadata we want to discard the common output path, so
    * `gs://open-targets-pre-data-releases/22.11/output/etl/parquet/reactome` becomes `/reactome`
    * and `gs://open-targets-pre-data-releases/22.11/output/etl/parquet/fda/adverseTargetReactions`
    * becomes `/fda/adverseTargetReactions`
    */
  private def getContentPath(path: String, outputFormat: String): String =
    path.split(outputFormat).last
}

class MetadataHelper(context: ETLSessionContext) extends MetadataWriter with LazyLogging {
  private val metadataSettings = context.configuration.common.metadata

  private def prepareMetadata(ioResource: IOResource): Metadata = {
    val data = ioResource.data
    val schema = data.schema.json
    val cols = data.columns.toList
    val id = ioResource.configuration.path.split("/").filter(_.nonEmpty).last
    val ioResourceConfig = ioResource.configuration.copy(
      path = getContentPath(ioResource.configuration.path, ioResource.configuration.format)
    )
    Metadata(id, ioResourceConfig, schema, cols)
  }

  def create(ioResource: IOResource): IOResource = {
    import context.sparkSession.implicits._
    val metadata = prepareMetadata(ioResource)
    val metadataDf: DataFrame =
      List(metadata).toDF
        .withColumn("timeStamp", current_timestamp())
        .coalesce(numPartitions = 1)

    val metadataIOResource = IOResource(
      metadataDf,
      metadataSettings.output.copy(path = s"${metadataSettings.output.path}/${metadata.id}")
    )

    metadataIOResource
  }

  def write(metadata: IOResource): Unit = {
    val data = metadata.data
    val conf = metadata.configuration
    logger.info(s"Save metadata: $conf")
    data.write.format(conf.format).mode(metadataSettings.writeMode).save(conf.path)
  }
}
