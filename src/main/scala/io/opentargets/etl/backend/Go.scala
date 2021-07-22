package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.{IOResource, IOResourceConfig, IoHelpers}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import io.opentargets.etl.preprocess.go.GoConverter
import org.apache.spark.sql.functions.{col, desc, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.jdk.CollectionConverters.asScalaIteratorConverter

object Go extends LazyLogging {
  def apply()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession

    logger.info("Executing Gene Ontology step.")

    logger.debug("Reading GO inputs")
    val inputs = Map("go" -> getGoDataFrame(context.configuration.geneOntology.goInput))

    logger.debug("Processing Gene Ontology")
    val goDF = inputs("go").data

    logger.debug("Writing Gene Ontology outputs")
    val dataframesToSave: IOResources = Map(
      "go" -> IOResource(goDF, context.configuration.geneOntology.output)
    )

    IoHelpers.writeTo(dataframesToSave)

  }

  def getGoDataFrame(io: IOResourceConfig)(implicit ss: SparkSession): IOResource = {
    import ss.implicits._
    val path: java.util.Iterator[String] = ss.read.textFile(io.path).toLocalIterator()
    val data = GoConverter.convertFileToGo(path.asScala)
    IOResource(data.toDF(), io)
  }

}
