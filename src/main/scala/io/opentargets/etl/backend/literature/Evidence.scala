package io.opentargets.etl.backend.literature

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.spark.Helpers.harmonicFn
import io.opentargets.etl.backend.spark.IOResource
import io.opentargets.etl.backend.spark.IoHelpers.{readFrom, writeTo}
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.Vectors.norm
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object Evidence extends Serializable with LazyLogging {
  val matchesSchema: StructType = StructType(
    Array(
      StructField(name = "datasourceId", dataType = StringType, nullable = false),
      StructField(name = "datatypeId", dataType = StringType, nullable = false),
      StructField(name = "targetFromSourceId", dataType = StringType, nullable = false),
      StructField(name = "diseaseFromSourceMappedId", dataType = StringType, nullable = false),
      StructField(name = "resourceScore", dataType = DoubleType, nullable = false),
      StructField(name = "similarity", dataType = DoubleType, nullable = false),
      StructField(name = "harmonicSimilarity", dataType = DoubleType, nullable = false),
      StructField(name = "sharedPublicationCount", dataType = IntegerType, nullable = false),
      StructField(name = "meanTargetFreqPerPub", dataType = DoubleType, nullable = false),
      StructField(name = "meanDiseaseFreqPerPub", dataType = DoubleType, nullable = false)
    )
  )

  val cooccurrencesSchema: StructType = StructType(
    Array(
      StructField(name = "targetFromSourceId", dataType = StringType, nullable = false),
      StructField(name = "diseaseFromSourceMappedId", dataType = StringType, nullable = false),
      StructField(name = "harmonicCooccurrenceSentiment", dataType = DoubleType, nullable = false),
      StructField(name = "cooccurredPublicationCount", dataType = IntegerType, nullable = false)
    )
  )

  def computeSimilarityScore(col1: Column, col2: Column): Column = {
    val cossim = udf { (v1: Vector, v2: Vector) =>
      val n1 = norm(v1, 2d)
      val n2 = norm(v2, 2d)
      val denom = n1 * n2
      if (denom == 0.0) 0.0
      else (v1 dot v2) / denom
    }

    cossim(col1, col2)
  }

  def computeEvidenceFromMatches(model: Word2VecModel,
                                 matches: DataFrame,
                                 threshold: Option[Double]
  )(implicit etlSessionContext: ETLSessionContext): DataFrame = {
    import etlSessionContext.sparkSession.implicits._

    val sectionImportances =
      etlSessionContext.configuration.literature.common.publicationSectionRanks
    val sectionRankTable =
      broadcast(
        sectionImportances
          .toDS()
          .orderBy($"rank".asc)
      )

    val gcols = List("pmid", "type", "keywordId")
    logger.info("filter matches by isMapped and only the sections in the rank list")
    val mWithV = matches
      .filter($"isMapped" === true)
      .join(sectionRankTable, Seq("section"))
      .groupBy(gcols.map(col): _*)
      .agg(count($"pmid").as("f"))
      .join(model.getVectors, $"word" === $"keywordId")
      .drop("word")

    val matchesDS = mWithV
      .filter($"type" === "DS")
      .drop("type")
      .withColumnRenamed("keywordId", "diseaseFromSourceMappedId")
      .withColumnRenamed("f", "diseaseF")
      .withColumnRenamed("vector", "diseaseV")
      .withColumnRenamed("vector", "diseaseV")
      .withColumnRenamed("pmid", "diseaseP")

    val matchesGP = mWithV
      .filter($"type" === "GP")
      .drop("type")
      .withColumnRenamed("keywordId", "targetFromSourceId")
      .withColumnRenamed("f", "targetF")
      .withColumnRenamed("vector", "targetV")
      .withColumnRenamed("pmid", "targetP")

    val ev = matchesDS
      .join(
        matchesGP,
        ($"targetP" === $"diseaseP") and ($"diseaseFromSourceMappedId" !== $"targetFromSourceId"),
        "inner"
      )
      .groupBy($"targetFromSourceId", $"diseaseFromSourceMappedId")
      .agg(
        first($"targetV").as("targetV"),
        first($"diseaseV").as("diseaseV"),
        mean($"targetF").as("meanTargetFreqPerPub"),
        mean($"diseaseF").as("meanDiseaseFreqPerPub"),
        count($"targetP").as("sharedPublicationCount")
      )
      .withColumn("sharedPublicationCount", $"sharedPublicationCount".cast(IntegerType))
      .withColumn("similarity", computeSimilarityScore($"targetV", $"diseaseV"))
      .filter($"similarity" > threshold.getOrElse(Double.MinPositiveValue))
      .withColumn("harmonicSimilarity",
                  harmonicFn(array_repeat($"similarity", $"sharedPublicationCount"))
      )
      .withColumn("resourceScore", $"harmonicSimilarity")
      .withColumn("datasourceId", lit("ew2v"))
      .withColumn("datatypeId", lit("literature"))
      .select(matchesSchema.fieldNames.map(col): _*)

    ev
  }

  def computeEvidenceFromCoocs(coocs: DataFrame, threshold: Option[Double])(implicit
      etlSessionContext: ETLSessionContext
  ): DataFrame = {
    import etlSessionContext.sparkSession.implicits._

    val gcols = List("targetFromSourceId", "diseaseFromSourceMappedId")
    logger.info("filter cooccurrences by isMapped and GP and DS and text size < 600")
    val aggregatedCoocs = coocs
      .filter(
        $"isMapped" === true and
          $"type1" === "GP" and
          $"type2" === "DS" and
          length($"text") < 600
      )
      .withColumn("cooccurrenceScore", $"evidence_score" / 10d)
      .withColumnRenamed("keywordId1", "targetFromSourceId")
      .withColumnRenamed("keywordId2", "diseaseFromSourceMappedId")
      .groupBy(gcols.map(col): _*)
      .agg(harmonicFn(collect_list($"cooccurrenceScore")).as("harmonicCooccurrenceSentiment"),
           countDistinct($"pmid").as("cooccurredPublicationCount")
      )
      .select(cooccurrencesSchema.fieldNames.map(col): _*)

    aggregatedCoocs
  }

  def generateEvidence(model: Word2VecModel,
                       matches: DataFrame,
                       coocs: DataFrame,
                       threshold: Option[Double]
  )(implicit etlSessionContext: ETLSessionContext): DataFrame = {

    val evMatches = computeEvidenceFromMatches(model, matches, threshold)
    val evCoocs = computeEvidenceFromCoocs(coocs, threshold)

    val joinCols = "targetFromSourceId" :: "diseaseFromSourceMappedId" :: Nil

    val joinedEvidences = evMatches.join(evCoocs, joinCols, "left_outer").na.fill(0d)

    joinedEvidences
  }

  def apply()(implicit context: ETLSessionContext): Unit = {
    implicit val ss: SparkSession = context.sparkSession

    val configuration = context.configuration.literature.evidence

    val imap = Map(
      "matches" -> configuration.inputs.matches,
      "coocs" -> configuration.inputs.cooccurrences
    )

    val matches = readFrom(imap).apply("matches").data
    val coocs = readFrom(imap).apply("coocs").data

    logger.info(s"Load w2v model from path ${configuration.inputs.model.path}")
    val m = Word2VecModel.load(configuration.inputs.model.path)

    logger.info("Generate evidence set from w2v model")
    val eset = generateEvidence(m, matches, coocs, configuration.threshold)
    val dataframesToSave = Map(
      "evidence" -> IOResource(eset, configuration.output)
    )

    writeTo(dataframesToSave)
  }
}
