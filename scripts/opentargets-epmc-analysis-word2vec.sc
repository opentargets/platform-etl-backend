import $file.resolvers
import $ivy.`ch.qos.logback:logback-classic:1.2.3`
import $ivy.`com.typesafe.scala-logging::scala-logging:3.9.2`
import $ivy.`com.typesafe:config:1.4.0`
import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
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


//
// export JAVA_OPTS="-Xms1G -Xmx24G"
// generate parquet matches and co-occurrences
// val epmcCooc = spark.read.json("\/data\/json\/*_mapped\/")
// .withColumn("sentence", explode($"sentences"))
// .selectExpr("*", "sentence.*").drop("sentence", "sentences", "matches")
// .filter($"co-occurrence".isNotNull)
// .withColumn("cooc", explode($"co-occurrence"))
// .selectExpr("*", "cooc.*").drop("cooc", "co-occurrence")
// .write.parquet("bla")
//
// val epmcMatches = spark.read.json("\/data\/json\/*_mapped\/")
// .withColumn("sentence", explode($"sentences"))
// .selectExpr("*", "sentence.*").drop("sentence", "sentences", "co-occurrence")
// .filter($"matches".isNotNull).withColumn("match", explode($"matches"))
// .selectExpr("*", "match.*").drop("match", "matches")
// .write.parquet("foo")
//
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

  def makeWord2VecModel(df: DataFrame,
                                groupCols: Seq[Column],
                                agg: (String, Column),
                                outputColName: String = "prediction"): Word2VecModel = {
    logger.info(s"compute FPGrowthModel for group cols ${groupCols.mkString("(", ", ", ")")}")
    val ar = df
      .groupBy(groupCols:_*)
      .agg(agg._2.as(agg._1))


    val w2vModel = new Word2Vec()
      .setInputCol(agg._1)
      .setOutputCol(outputColName)

    val model = w2vModel.fit(ar)

    // Display frequent itemsets.
    model.getVectors.show(25, false)

    model
  }
}

object ETL extends LazyLogging {
  def apply(matches: String, coocs: String, output: String) = {
    import SparkSessionWrapper._
    import session.implicits._

    logger.info("fit the parametrised model and generate it to apply later to another DF")
    val mDF = session.read.parquet(matches)
    val matchesModel = makeWord2VecModel(mDF.filter($"isMapped" === true),
      groupCols = Seq($"pmid"),
      agg = "items" -> array_distinct(collect_list($"keywordId")),
      outputColName = "predictions"
    )

    logger.info("saving the generated model for Word2Vec")
    matchesModel.save(output + "/matchesW2V")

    // TODO HERE PROPER AGGREGATION
    logger.info("load the co occurrences from parquet")
    val groupedKeys = Seq($"type1", $"type2", $"keywordId1", $"keywordId2")
    val df = session.read.parquet(coocs)

    logger.info("filter co occurrences only mapped ones and rescale 'evidence_score' between 0..1")
    val assocs = df
      .filter($"isMapped" === true)

    logger.info("compute the predictions to the associations DF with the precomputed model FPGrowth")
    val assocsWithPredictions = matchesModel
      .transform(
        assocs.select(groupedKeys:_*)
          .withColumn("items", array($"keywordId1", $"keywordId2"))
      )

    logger.info("saving computed predictions to the associations df")
    assocsWithPredictions.write.parquet(output + "/associationsWithPredictions")
  }
}

@main
  def main(matches: String, coocs: String, output: String): Unit =
    ETL(matches, coocs, output)
