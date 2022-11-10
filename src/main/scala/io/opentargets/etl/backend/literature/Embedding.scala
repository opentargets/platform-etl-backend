package io.opentargets.etl.backend.literature

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.Configuration.{LiteratureModelConfiguration, OTConfig}
import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.spark.IOResource
import io.opentargets.etl.backend.spark.IoHelpers.{readFrom, writeTo}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.sql.expressions.Window

object Embedding extends Serializable with LazyLogging {
  private def filterMatches(
      matches: DataFrame
  )(implicit etlSessionContext: ETLSessionContext): DataFrame = {
    import etlSessionContext.sparkSession.implicits._

    logger.info("prepare matches filtering by type of entities")

    val types = "DS" :: "GP" :: "CD" :: Nil
    matches
      .filter($"isMapped" === true and $"type".isInCollection(types))
  }

  private def regroupMatches(
      selectCols: Seq[String]
  )(df: DataFrame)(implicit etlSessionContext: ETLSessionContext): DataFrame = {
    import etlSessionContext.sparkSession.implicits._

    logger.info("prepare matches regrouping the entities by ranked section")
    val sectionImportances =
      etlSessionContext.configuration.literature.common.publicationSectionRanks
    val sectionRankTable =
      broadcast(
        sectionImportances
          .toDS()
          .orderBy($"rank".asc)
      )

    val partitionPerSection = "pmid" :: "rank" :: Nil
    val wPerSection = Window.partitionBy(partitionPerSection.map(col): _*)

    val trDS = df
      .join(sectionRankTable, Seq("section"))
      .withColumn("keys", collect_set($"keywordId").over(wPerSection))
      .dropDuplicates(partitionPerSection.head, partitionPerSection.tail: _*)
      .groupBy($"pmid")
      .agg(collect_list($"keys").as("keys"))
      .withColumn("overall", flatten($"keys"))
      .withColumn("all", concat($"keys", array($"overall")))
      .withColumn("terms", explode($"all"))
      .selectExpr(selectCols: _*)
      .persist()

    logger.info("saving training dataset")
    writeTo(
      Map(
        "trainingSet" -> IOResource(
          trDS,
          etlSessionContext.configuration.literature.embedding.outputs.trainingSet
        )
      )
    )(etlSessionContext)

    trDS

  }

  def makeWord2VecModel(
      df: DataFrame,
      modelConfiguration: LiteratureModelConfiguration,
      inputColName: String,
      outputColName: String = "prediction"
  ): Word2VecModel = {
    logger.info(s"compute Word2Vec model for input col $inputColName into $outputColName")

    val w2vModel = new Word2Vec()
      .setWindowSize(modelConfiguration.windowSize)
      .setNumPartitions(modelConfiguration.numPartitions)
      .setMaxIter(modelConfiguration.maxIter)
      .setMinCount(modelConfiguration.minCount)
      .setStepSize(modelConfiguration.stepSize)
      .setInputCol(inputColName)
      .setOutputCol(outputColName)

    val model = w2vModel.fit(df)

    model
  }

  def generateModel(
      matches: DataFrame
  )(implicit etlSessionContext: ETLSessionContext): Word2VecModel = {
    val modelConfiguration = etlSessionContext.configuration.literature.embedding.modelConfiguration
    val df = matches
      .transform(filterMatches)
      .transform(regroupMatches("pmid" :: "terms" :: Nil))

    logger.info(s"training W2V model with configuration ${modelConfiguration.toString}")
    makeWord2VecModel(df, modelConfiguration, inputColName = "terms", outputColName = "synonyms")
  }

  def compute(matches: DataFrame, configuration: OTConfig)(implicit
      etlSessionContext: ETLSessionContext
  ): Map[String, IOResource] = {

    val output = configuration.literature.embedding.outputs.model
    val modelConf = configuration.literature.embedding.modelConfiguration

    logger.info("CPUs available: " + Runtime.getRuntime().availableProcessors().toString())
    logger.info(s"Model configuration: ${modelConf.toString}")

    val matchesModels = generateModel(matches)

    // The matchesModel is a W2VModel and the output is parquet.
    configuration.sparkSettings.writeMode match {
      case "overwrite" => matchesModels.write.overwrite().save(output.path)
      case _           => matchesModels.save(output.path)
    }

    val dataframesToSave = Map.empty[String, IOResource]
    dataframesToSave
  }

  def apply()(implicit context: ETLSessionContext): Unit = {
    implicit val ss: SparkSession = context.sparkSession

    logger.info("Embedding step reading the files matches")
    val configuration = context.configuration

    val mappedInputs = Map(
      "matches" -> configuration.literature.embedding.input
    )
    val inputDataFrames = readFrom(mappedInputs)
    compute(inputDataFrames("matches").data, configuration)
  }
}
