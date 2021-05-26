// how to get the vectors, operate and find synonyms
// val vectors = model.getVectors
//  .filter($"word" isInCollection(Seq(pi3k, atk1, "ENSG00000105221", "ENSG00000140992", "ENSG00000152256")))
//  .agg(Summarizer.sum($"vector").as("v")).select("v").collect.head.getAs[Vector]("v")
// model.findSynonyms(vectors, 10).show()

import $ivy.`ch.qos.logback:logback-classic:1.2.3`
import $ivy.`com.typesafe.scala-logging::scala-logging:3.9.2`
import $ivy.`com.typesafe:config:1.4.0`
import $ivy.`com.github.fommil.netlib:all:1.1.2`
import $ivy.`org.apache.spark::spark-core:3.1.1`
import $ivy.`org.apache.spark::spark-mllib:3.1.1`
import $ivy.`org.apache.spark::spark-sql:3.1.1`
import $ivy.`com.github.pathikrit::better-files:3.8.0`
import $ivy.`com.typesafe.play::play-json:2.9.1`
import $ivy.`com.github.haifengl:smile-mkl:2.6.0`
import $ivy.`com.github.haifengl::smile-scala:2.6.0`
import $ivy.`com.johnsnowlabs.nlp:spark-nlp_2.12:3.0.3`
import breeze.linalg.Vector.{castFunc, castOps}
import org.apache.spark.broadcast._
import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel
import org.apache.spark.ml.fpm._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.ml.functions._
import org.apache.spark.ml.Pipeline
import smile.math._
import smile.math.matrix.{matrix => m}
import com.johnsnowlabs.nlp.annotator._
import com.johnsnowlabs.nlp.pretrained._
import com.johnsnowlabs.nlp._
import org.apache.spark.ml.linalg._
import breeze.linalg.functions.cosineDistance
import breeze.linalg.{DenseVector => BDV}

object SparkSessionWrapper extends LazyLogging {
  logger.info("Spark Session init")
  lazy val sparkConf = new SparkConf()
    .set("spark.driver.maxResultSize", "0")
    .set("spark.debug.maxToStringFields", "2000")
    .set("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
    .setAppName("etl-generation")
    .setMaster("local[*]")

  lazy val spark: SparkSession =
    SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate
}

object ETL extends LazyLogging {
  def toBreeze( dv: DenseVector ): BDV[Double] =
    new BDV[Double](dv.values)

  val cosFn = (v1: DenseVector, v2: DenseVector) => cosineDistance(toBreeze(v1), toBreeze(v2))
  val scoreFn = udf(cosFn)

  def apply(prefix: String, labelsUri: String, model: String, filteredModel: String, output: String): Unit = {
    import SparkSessionWrapper._
    import spark.implicits._
    implicit val ss: SparkSession = spark

    logger.info("load required datasets from ETL parquet format")
    val diseases = broadcast(
      spark.read.parquet(s"${prefix}/diseases")
        .selectExpr("lower(id) as efoId", "name as efoName")
        .orderBy($"efoId")
    )

    logger.info("load labels")
    val labels = spark.read.parquet(labelsUri).selectExpr("labelsTerms_sentence as sentence", "labelsKey as words")

    logger.info("load model1")
    val m = Word2VecModel
      .load(model)
      .setInputCol("words")
      .setOutputCol("v")

    // this is the same model but filtered just ontology terms so we can speed up the similarity
    // to the whole label dataset (basically load the data from the previous model, filter and write
    // and then copy the model folder and replace manually the filtered data in parquet
    logger.info("load model2")
    val m2 = Word2VecModel.load(filteredModel)

    logger.info("transform labels sentences into averaged vectors")
    val lv = m.transform(labels)
      .filter($"v".isNotNull)

    val w = Window.partitionBy("sentence").orderBy($"score".desc)
    val m2V = broadcast(m2.getVectors)

    logger.info("apply UDF fn to find top N synonyms")
    val resolvedLabels = lv.crossJoin(m2V)
      .withColumn("score", scoreFn($"v", $"vector"))
      .selectExpr("sentence", "lower(word) as efoId", "score")
      .filter($"score" > 0.1)
      .withColumn("rank", dense_rank().over(w))
      .filter($"rank" <= 5)
      .join(diseases, Seq("efoId"))
      .orderBy($"sentence".asc, $"rank".asc)

    logger.info("save resolved labels")
    resolvedLabels.write.parquet(s"$output/resolved_labels")

  }
}

@main
def main(prefix: String,
         labels: String,
         model: String,
         filteredModel: String,
         output: String): Unit =
  ETL(prefix, labels, model, filteredModel, output)
