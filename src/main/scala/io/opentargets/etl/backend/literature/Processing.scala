package io.opentargets.etl.backend.literature

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.Configuration.LiteratureProcessing
import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.spark.IoHelpers.writeTo
import io.opentargets.etl.backend.spark.IOResource
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.LongType
import org.apache.spark.storage.StorageLevel

import scala.language.postfixOps

object Processing extends Serializable with LazyLogging {

  private def harmonicFn(v: Column, s: Column): Column =
    aggregate(
      zip_with(v, sequence(lit(1), s), (e1, e2) => e1 / pow(e2, 2d)),
      lit(0d),
      (c1, c2) => c1 + c2
    )

  private def filterCooccurrences(df: DataFrame, isMapped: Boolean)(implicit
      sparkSession: SparkSession
  ): DataFrame = {
    import sparkSession.implicits._

    val droppedCols = "co-occurrence" :: Nil

    df.selectExpr("*", "`co-occurrence`.*")
      .drop(droppedCols: _*)
      .filter($"isMapped" === isMapped)

  }

  private def filterMatches(df: DataFrame, isMapped: Boolean)(implicit
      sparkSession: SparkSession
  ): DataFrame = {
    import sparkSession.implicits._

    val droppedCols = "match" :: Nil

    df.selectExpr("*", "match.*")
      .drop(droppedCols: _*)
      .filter($"isMapped" === isMapped)
  }

  private def filterMatchesForCH(
      matchesDF: DataFrame
  )(implicit context: ETLSessionContext): (DataFrame, DataFrame) = {
    import context.sparkSession.implicits._

    val sectionImportances = context.configuration.literature.common.publicationSectionRanks
    val titleWeight = sectionImportances.withFilter(_.section == "title").map(_.weight).head

    val sectionRankTable =
      broadcast(
        sectionImportances
          .toDS()
          .orderBy($"rank".asc)
      )

    val wBySectionKeyword = Window.partitionBy("pmid", "section", "keywordId")
    val wByKeyword = Window.partitionBy("pmid", "keywordId")

    val cols = List(
      "pmid",
      "pmcid",
      "date",
      "year",
      "month",
      "day",
      "keywordId",
      "relevance",
      "keywordType"
    )

    val matchesDF2 = matchesDF
      .withColumnRenamed("type", "keywordType")

    val sentencesDF = matchesDF2
      .filter($"section".isInCollection(Seq("title", "abstract")))
      .filter(col("pmid") isNotNull)
      .drop("date",
            "year",
            "month",
            "day",
            "isMapped",
            "organisms",
            "pubDate",
            "text",
            "trace_source",
            "labelN"
      )

    val litDF = matchesDF2
      .join(sectionRankTable, Seq("section"), "left_outer")
      .na
      .fill(100, "rank" :: Nil)
      .na
      .fill(0.01, "weight" :: Nil)
      .withColumn("keywordSectionV",
                  when($"section" =!= "title", collect_list($"weight").over(wBySectionKeyword))
                    .otherwise(array(lit(titleWeight)))
      )
      .dropDuplicates("pmid", "section", "keywordId")
      .withColumn("relevanceV",
                  flatten(collect_list($"keywordSectionV").over(wByKeyword.orderBy($"rank".asc)))
      )
      .withColumn("relevance", harmonicFn($"relevanceV", size($"relevanceV")))
      .dropDuplicates("pmid", "keywordId")
      .selectExpr(cols: _*)

    (litDF, sentencesDF)
  }

  def apply()(implicit context: ETLSessionContext): Map[String, IOResource] = {
    implicit val ss: SparkSession = context.sparkSession

    logger.info("Processing step")

    val processingConfiguration: LiteratureProcessing = context.configuration.literature.processing
    val grounding: Map[String, DataFrame] = Grounding.compute(processingConfiguration)

    logger.info("Processing raw evidences and persist matches and cooccurrences")

    ("matches" :: "cooccurrences" :: Nil) foreach { l =>
      grounding(l).persist(StorageLevel.MEMORY_AND_DISK)
    }

    logger.info("Processing matches calculate done")
    val matches = filterMatches(grounding("matches"), isMapped = true)

    logger.info("Processing coOccurences calculate done")
    val coocs = filterCooccurrences(grounding("cooccurrences"), isMapped = true)

    val literatureIndexAlt: (DataFrame, DataFrame) = filterMatchesForCH(matches)

    val outputs = processingConfiguration.outputs
    val dataframesToSave = Map(
      "cooccurrences" -> IOResource(coocs, outputs.cooccurrences),
      "matches" -> IOResource(matches, outputs.matches),
      "literatureIndex" -> IOResource(literatureIndexAlt._1, outputs.literatureIndex),
      "publicationSentences" -> IOResource(literatureIndexAlt._2, outputs.literatureSentences)
    ) ++ {
      if (processingConfiguration.writeFailures) {
        Map(
          "failedMatches" -> IOResource(
            grounding("matchesFailed"),
            outputs.matches.copy(path = outputs.failedMatches.path)
          ),
          "failedCoocs" -> IOResource(
            grounding("cooccurrencesFailed"),
            outputs.matches.copy(path = outputs.failedCooccurrences.path)
          )
        )
      } else Map.empty
    }
    logger.info(s"Write literatures outputs: ${dataframesToSave.keySet}")

    writeTo(dataframesToSave)
    dataframesToSave
  }

}
