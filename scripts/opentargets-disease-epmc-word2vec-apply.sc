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
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.stat.Summarizer

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
  val applyModelFn = (model: Broadcast[Word2VecModel], vector: DenseVector) => {
    try {
      model.value
        .findSynonymsArray(vector, 10)
    } catch {
      case _: Throwable => Array.empty[(String, Double)]
    }
  }

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

    val prefixes = diseases
      .withColumn("prefix", split($"efoId", "_").getItem(0))
      .select("prefix")
      .distinct.as[String].collect
    val labels = spark.read.parquet(labelsUri).selectExpr("labelsTerms_sentence", "explode(labelsKey) as word")
    val m = Word2VecModel.load(model)

    // this is the same model but filtered just ontology terms so we can speed up the similarity
    // to the whole label dataset (basically load the data from the previous model, filter and write
    // and then copy the model folder and replace manually the filtered data in parquet
    val m2 = Word2VecModel.load(filteredModel)
    val bm = spark.sparkContext.broadcast(m2)

    val labelsFn = udf(applyModelFn(bm, _))
    val lv = labels
      .join(broadcast(m.getVectors.orderBy($"word")), Seq("word"),"left_outer")
      .filter($"vector".isNotNull)
      .groupBy($"labelsTerms_sentence")
      .agg(Summarizer.sum($"vector").as("v"))

    val resolvedLabels = lv
      .withColumn("synonyms", labelsFn($"v"))
      .withColumn("synonym", explode($"synonyms"))
      .withColumn("synonymId", $"synonym".getField("_1"))
      .withColumn("synonymScore", $"synonym".getField("_2"))
      .selectExpr("labelsTerms_sentence as label", "synonymId", "synonymScore")
      .join(diseases, $"efoId" === $"synonymId")
      .drop("efoId")
      .orderBy($"label".asc, $"synonymScore".desc)

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
