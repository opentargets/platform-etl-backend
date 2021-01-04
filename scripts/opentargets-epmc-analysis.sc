import $file.resolvers

import $ivy.`ch.qos.logback:logback-classic:1.2.3`
import $ivy.`com.typesafe.scala-logging::scala-logging:3.9.2`
import $ivy.`com.typesafe:config:1.4.0`
import org.apache.spark.storage.StorageLevel
// import $ivy.`com.github.fommil.netlib:all:1.1.2`
import $ivy.`org.apache.spark::spark-core:3.0.1`
import $ivy.`org.apache.spark::spark-mllib:3.0.1`
import $ivy.`org.apache.spark::spark-sql:3.0.1`
import $ivy.`com.github.pathikrit::better-files:3.8.0`
import $ivy.`com.typesafe.play::play-json:2.9.1`
import $ivy.`graphframes:graphframes:0.8.1-spark3.0-s_2.12`

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.ml._
import org.apache.spark.ml.fpm._
import com.typesafe.scalalogging.LazyLogging

import org.graphframes._

object SparkSessionWrapper extends LazyLogging {
  logger.info("Spark Session init")
  lazy val sparkConf = new SparkConf()
    .set("spark.driver.maxResultSize", "0")
    .set("spark.debug.maxToStringFields", "2000")
    .set("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
    .setAppName("etl-generation")
    .setMaster("local[*]")

  lazy val session: SparkSession =
    SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate

  def harmonicFn(c: Column): Column =
    aggregate(
      zip_with(
        sort_array(c, asc = false),
        sequence(lit(1), size(c)),
        (e1, e2)  => e1 / pow(e2, 2D)),
      lit(0D),
      (c1, c2) => c1 + c2
    )

  /** compute a model to calculate suggestions based on FGrowth AR and then the model can be used to
    * generate suggestions to an array of ids (targets, diseases, drugs)
    *
    * @param df
    * @param groupCols
    * @param agg
    * @param minSupport the minimum support for an itemset to be identified as frequent.
    *                   For example, if an item appears 3 out of 5 transactions,
    *                   it has a support of 3/5=0.6.
    * @param minConfidence minimum confidence for generating Association Rule.
    *                      Confidence is an indication of how often an association
    *                      rule has been found to be true. For example, if in the transactions
    *                      itemset X appears 4 times, X and Y co-occur only 2 times,
    *                      the confidence for the rule X => Y is then 2/4 = 0.5. The
    *                      parameter will not affect the mining for frequent itemsets, but
    *                      specify the minimum confidence for generating association rules from
    *                      frequent itemsets.
    * @return the generated FPGrowthModel
    */
  def makeAssociationRulesModel(df: DataFrame,
                                groupCols: Seq[Column],
                                agg: (String, Column),
                                minSupport: Double = 0.1,
                                minConfidence: Double = 0.3,
                                outputColName: String = "prediction"): FPGrowthModel = {
    logger.info(s"compute FPGrowthModel for group cols ${groupCols.mkString("(", ", ", ")")}")
    val ar = df
      .groupBy(groupCols:_*)
      .agg(agg._2.as(agg._1))


    val fpgrowth = new FPGrowth()
      .setItemsCol(agg._1)
      .setMinSupport(minSupport)
      .setMinConfidence(minConfidence)
      .setPredictionCol(outputColName)

    val model = fpgrowth.fit(ar)

    // Display frequent itemsets.
    model.freqItemsets.show(25, false)
    model.associationRules.show(25, false)

    model
  }

  def makeStatsPerTerm(df: DataFrame, groupCols: Seq[Column]): DataFrame = {
    import session.implicits._
    logger.info(s"compute few stats per match in the matches DF grouping by ${groupCols.mkString("(", ", ", ")")}")
    val groups = df.groupBy(groupCols:_*)
      .agg(
        first($"label").as("label"),
        countDistinct($"pmid").as("f"),
        count($"pmid").as("N")
      )

    groups
  }

  def makeAssociations(df: DataFrame, groupCols: Seq[Column]): DataFrame = {
    import session.implicits._
    logger.info(s"compute associations with desc. stats and harmonic ${groupCols.mkString("(", ", ", ")")}")
    val assocs = df.groupBy(groupCols:_*)
      .agg(
        first($"label1").as("label1"),
        first($"label2").as("label2"),
        countDistinct($"pmid").as("f"),
        mean($"evidence_score").as("mean"),
        stddev($"evidence_score").as("std"),
        max($"evidence_score").as("max"),
        min($"evidence_score").as("min"),
        expr("approx_percentile(evidence_score, array(0.25, 0.5, 0.75))").as("q"),
        count($"pmid").as("N"),
        collect_list($"evidence_score").as("evidenceScores")
      )
      .withColumn("median", element_at($"q", 2))
      .withColumn("q1", element_at($"q", 1))
      .withColumn("q3", element_at($"q", 3))
      .withColumn("harmonic", harmonicFn($"evidenceScores"))
      .drop("evidenceScores", "q")

    assocs
  }
}

object ETL extends LazyLogging {
  def apply(matches: String, coocs: String, output: String): Unit = {
    import SparkSessionWrapper._
    import session.implicits._

    val mDF = session.read.parquet(matches)
    val matchesModel = makeAssociationRulesModel(mDF.filter($"isMapped" === true),
      groupCols = Seq($"pmid"),
      agg = "items" -> array_distinct(collect_list($"keywordId")),
      minSupport = 0.01,
      minConfidence = 0.03,
      outputColName = "predictions"
    )

    logger.info("saving the generated model for FPGrowth Association Rules")
    matchesModel.save(output + "/matchesFPM")

    val groupedKeys = Seq($"type1", $"type2", $"keywordId1", $"keywordId2")
    val df = session.read.parquet(coocs)

    logger.info("read EPMC co-occurrences dataset, filter only mapped ones and rescale score between 0..1")
    val assocs = df
      .filter($"isMapped" === true)
      .withColumn("evidence_score", array_min(array($"evidence_score" / 10D, lit(1D))))
      .transform(makeAssociations(_, groupedKeys))

    logger.info("saving associations dataset")
    assocs.write.parquet(output + "/associations")

    logger.info("compute the predictions to the associations DF with the precomputed model FPGrowth")
    val assocsWithPredictions = matchesModel
      .transform(
        assocs.select(groupedKeys:_*)
          .withColumn("items", array($"keywordId1", $"keywordId2"))
      )

    logger.info("saving computed predictions to the associations df")
    assocsWithPredictions.write.parquet(output + "/associationsWithPredictions")

    logger.info("generate assocs but group also per year and month")
    val groupedKeysWithDate = Seq($"year", $"type1", $"type2", $"keywordId1", $"keywordId2")
    val assocsPerYearMonth = df
      .withColumn("year", year($"pubDate"))
      .withColumn("month", month($"pubDate"))
      .filter($"isMapped" === true and $"year".isNotNull and $"month".isNotNull)
      .withColumn("evidence_score", array_min(array($"evidence_score" / 10D, lit(1D))))
      .transform(makeAssociations(_, groupedKeysWithDate))

    logger.info("save associations by time (year, month)")
    assocsPerYearMonth.write.parquet(output + "/associationsWithTimeSeries")

    logger.info("generate match stats but group also per year and month")
    val groupedKeysWithDateForMatches = Seq($"year", $"type", $"keywordId")
    val matchesPerYearMonth = mDF
      .withColumn("year", year($"pubDate"))
      .withColumn("month", month($"pubDate"))
      .filter($"isMapped" === true and $"year".isNotNull and $"month".isNotNull)
      .transform(makeStatsPerTerm(_, groupedKeysWithDateForMatches))

    logger.info("save matches stats per (year month)")
    matchesPerYearMonth.write.parquet(output + "/matchesWithTimeSeries")
  }
}

@main
  def main(matches: String, coocs: String, output: String): Unit =
    ETL(matches, coocs, output)
