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

object OpentargetsFunctions extends LazyLogging {
  logger.info("Spark Session init")
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
    logger.info(s"compute few stats per match in the matches DF grouping by ${groupCols.mkString("(", ", ", ")")}")
    val groups = df.groupBy(groupCols:_*)
      .agg(
        first(col("label")).as("label"),
        countDistinct(col("pmid")).as("f"),
        count(col("pmid")).as("N")
      )

    groups
  }

  def makeIndirect(df: DataFrame, dfId: String, disease: DataFrame, diseaseId: String): DataFrame = {
    val dis = disease.selectExpr(s"${diseaseId} as _id", "ancestors")
      .withColumn("ids",
        array_union(
          array(col("_id")),
          coalesce(col("ancestors"), typedLit(Seq.empty[String]))))
      .withColumn(dfId, explode(col("ids")))
      .drop("ancestors", "ids")

    df.join(dis, Seq(dfId))
      .withColumnRenamed(dfId, "__del")
      .withColumnRenamed("_id", dfId)
      .drop("__del")
  }

  def makeAssociations(df: DataFrame, groupCols: Seq[Column]): DataFrame = {
    logger.info(s"compute associations with desc. stats and harmonic ${groupCols.mkString("(", ", ", ")")}")
    val assocs = df.groupBy(groupCols:_*)
      .agg(
        countDistinct(col("pmid")).as("f"),
        mean(col("evidence_score")).as("mean"),
        stddev(col("evidence_score")).as("std"),
        max(col("evidence_score")).as("max"),
        min(col("evidence_score")).as("min"),
        expr("approx_percentile(evidence_score, array(0.25, 0.5, 0.75))").as("q"),
        count(col("pmid")).as("N"),
        harmonicFn(collect_list(col("evidence_score"))).as("harmonic")
      )
      .withColumn("median", element_at(col("q"), 2))
      .withColumn("q1", element_at(col("q"), 1))
      .withColumn("q3", element_at(col("q"), 3))
      .drop("q")

    assocs
  }

  def makeWord2VecModel(df: DataFrame,
                        inputColName: String,
                        outputColName: String = "prediction"): Word2VecModel = {
    logger.info(s"compute Word2Vec model for input col ${inputColName} into ${outputColName}")

    val w2vModel = new Word2Vec()
      .setNumPartitions(32)
      .setMaxIter(10)
      .setInputCol(inputColName)
      .setOutputCol(outputColName)

    val model = w2vModel.fit(df)

    // Display frequent itemsets.
    model.getVectors.show(25, false)

    model
  }

  //https://stackoverflow.com/questions/52034650/scala-spark-calculate-grouped-by-auc
  //def trapezoid(points: Seq[(Double, Double)]): Double = {
  //    require(points.length == 2)
  //    val x = points.head
  //    val y = points.last
  //    (y._1 - x._1) * (y._2 + x._2) / 2.0
  //}
  //
  //def areaUnderCurve(curve: Iterable[(Double, Double)]): Double = {
  //    curve.toIterator.sliding(2).withPartial(false).aggregate(0.0)(
  //      seqop = (auc: Double, points: Seq[(Double, Double)]) => auc + trapezoid(points),
  //      combop = _ + _
  //    )
  //}

  //val data = Seq(("id1", Array((0.5, 1.0), (0.6, 0.0), (0.7, 1.0), (0.8, 0.0))), ("id2", Array((0.5, 1.0), (0.6, 0.0), (0.7, 1.0), (0.8, 0.3)))).toDF("key","values")
  //
  //case class Record(key : String, values : Seq[(Double,Double)])
  //
  //data.as[Record].map(r => (r.key, r.values, areaUnderCurve(r.values))).show
  //// +---+--------------------+-------------------+
  //// | _1|                  _2|                 _3|
  //// +---+--------------------+-------------------+
  //// |id1|[[0.5, 1.0], [0.6...|0.15000000000000002|
  //// |id2|[[0.5, 1.0], [0.6...|0.16500000000000004|
  //// +---+--------------------+-------------------+
}
