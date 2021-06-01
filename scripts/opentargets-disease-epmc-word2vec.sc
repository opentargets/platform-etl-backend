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
  // https://meta.wikimedia.org/wiki/Stop_word_list/google_stop_word_list#English
  val googleStopWords: Array[String] = ("about above after again against all am an and any are aren't as at be because " +
    "been before being below between both but by can't cannot could couldn't did didn't do does doesn't doing don't down " +
    "during each few for from further had hadn't has hasn't have haven't having he he'd he'll he's her here here's hers " +
    "herself him himself his how how's i'd i'll i'm i've if in into is isn't it it's its itself let's me more most mustn't " +
    "my myself no nor not of off on once only or other ought our ours ourselves out over own same shan't she she'd she'll " +
    "she's should shouldn't so some such than that that's the their theirs them themselves then there there's these they " +
    "they'd they'll they're they've this those through to too under until up very was wasn't we we'd we'll we're we've " +
    "were weren't what what's when when's where where's which while who who's whom why why's with won't would wouldn't " +
    "you you'd you'll you're you've your yours yourself yourselves").split(" ")

  val allStopWords: Array[String] = Array("a", "i") ++ googleStopWords ++ googleStopWords.map(_.capitalize)

  val columnsToInclude = List(
//    "document",
//    "documentNormaliser",
    "sentence",
//    "token",
//    "stop",
//    "clean",
    "stem",
//    "pos"
  )

  private def generatePipeline(fromCol: String, columns: List[String]): Pipeline = {
    // https://nlp.johnsnowlabs.com/docs/en/models#english---models
    val documentAssembler = new DocumentAssembler()
      .setInputCol(fromCol)
      .setOutputCol("document")

    val documentNormalizer = new DocumentNormalizer()
      .setInputCols("document")
      .setOutputCol("documentNormaliser")

    val sentenceDetector = new SentenceDetector()
      .setInputCols("documentNormaliser")
      .setOutputCol("sentence")
      .setExplodeSentences(true)

    val tokenizer = new Tokenizer()
      .setSplitChars(Array("-", "/", ":", ",", ";"))
      .setInputCols("sentence")
      .setOutputCol("token")

    val cleaner = new StopWordsCleaner()
      .setCaseSensitive(true)
      .setStopWords(allStopWords)
      .setInputCols("token")
      .setOutputCol("stop")

    val normaliser = new Normalizer()
      .setInputCols("stop")
      .setOutputCol("clean")
      .setLowercase(true)
      .setCleanupPatterns(Array("[^\\w\\d\\s]", "[-]", "[/]"))

    val stemmer = new Stemmer()
      .setInputCols("clean")
      .setOutputCol("stem")

    val posTagger = PerceptronModel.pretrained("pos_anc", "en")
      .setInputCols("sentence", "clean")
      .setOutputCol("pos")

//    val sentenceEmbeddings = UniversalSentenceEncoder.pretrained("tfhub_use", "en")
//      .setInputCols("document")
//      .setOutputCol("sentence_embedding")
//
//    val sentimentDetector = SentimentDLModel.pretrained("sentimentdl_use_imdb", "en")
//      .setInputCols("sentence_embedding")
//      .setOutputCol("sentiment")
//
//    val embeddings = BertEmbeddings.pretrained(name="bert_base_cased", "en")
//      .setInputCols("document", "clean")
//      .setOutputCol("embeddings")
//
//    val ner = NerDLModel.pretrained("ner_dl_bert", "en")
//      .setInputCols(Array("document", "clean", "embeddings"))
//      .setOutputCol("ner")

    val finisher = new Finisher()
      .setInputCols(columns:_*)

    val pipeline = new Pipeline()
      .setStages(
        Array(
          documentAssembler,
          documentNormalizer,
          sentenceDetector,
          tokenizer,
          cleaner,
          normaliser,
          stemmer,
          posTagger,
//          embeddings,
//          sentenceEmbeddings,
//          sentimentDetector,
//          ner,
          finisher
        )
      )

    pipeline
  }

  private def normaliseSentence(df: DataFrame, pipeline: Pipeline, columnNamePrefix: String,
                                columns: List[String]): DataFrame = {
    val annotations = pipeline
      .fit(df)
      .transform(df)

    val transCols = columns.map( c => {
      s"finished_$c" -> s"${columnNamePrefix}_$c"
    })

    transCols.foldLeft(annotations) {
      (B, p) => B.withColumnRenamed(p._1, p._2)
    }
  }

  val translateFn = (c: Column) => translate(c, "-", " ")

  def loadMatchesText(path: String)(implicit sparkSession: SparkSession): DataFrame = {
    sparkSession.read.parquet(path).selectExpr("text").dropDuplicates()
  }

  def loadMatches(path: String)(implicit sparkSession: SparkSession): DataFrame = {
    sparkSession.read.parquet(path).selectExpr("keywordId", "label as text").dropDuplicates()
  }

  def loadLabels(path: String)(implicit sparkSession: SparkSession): DataFrame =
    sparkSession.read.parquet(path).selectExpr("label", "text")

  def generateUniqueLabels(df: DataFrame): DataFrame =
    df.select("label").distinct().orderBy(col("label").asc)

  def generateUniqueTexts(df: DataFrame): DataFrame =
    df.select("text").dropDuplicates()

  def apply(prefix: String, labels: String, matches: String, output: String): Unit = {
    import SparkSessionWrapper._
    import spark.implicits._
    implicit val ss: SparkSession = spark

    logger.info("load required datasets from ETL parquet format")
    val diseases = spark.read.parquet(s"${prefix}/diseases")

    val pipeline = generatePipeline("text", columnsToInclude)

    val D = diseases
      .selectExpr("id as efoId", "name as efoName", "synonyms.*")
      .withColumn("exactSynonyms", transform(expr("coalesce(hasExactSynonym, array())"),
        c => struct(c.as("key"), lit(0.999).as("factor"))))
      .withColumn("narrowSynonyms", transform(expr("coalesce(hasNarrowSynonym, array())"),
        c => struct(c.as("key"), lit(0.998).as("factor"))))
      .withColumn("broadSynonyms", transform(expr("coalesce(hasBroadSynonym, array())"),
        c => struct(c.as("key"), lit(0.997).as("factor"))))
      .withColumn("relatedSynonyms", transform(expr("coalesce(hasRelatedSynonym, array())"),
        c => struct(c.as("key"), lit(0.996).as("factor"))))
      .withColumn("_text",
        explode(flatten(array(array(struct($"efoName".as("key"), lit(1d).as("factor"))), $"broadSynonyms", $"exactSynonyms", $"narrowSynonyms", $"relatedSynonyms"))))
      .withColumn("text", $"_text".getField("key"))
      .withColumn("factor", $"_text".getField("factor"))
      .drop("exactSynonyms", "narrowSynonyms", "broadSynonyms", "relatedSynonyms", "_text")
      .filter($"text".isNotNull and length($"text") > 0)

    val labelsDF = loadLabels(labels)
    val matchesDF = loadMatches(matches)
    val matchesTextDF = loadMatchesText(matches)

    val DN = D.transform(normaliseSentence(_, pipeline, "efoTerms", columnsToInclude))
      .drop("text")
      .withColumn("efoKey", transform($"efoTerms_stem", lower _))
      .filter($"efoKey".isNotNull and size($"efoKey") > 0)
      .withColumn("terms", concat(array($"efoId"), $"efoKey"))

    val MN = matchesDF.transform(normaliseSentence(_, pipeline, "matchesTerms", columnsToInclude))
      .drop("text")
      .withColumn("matchesKey", transform($"matchesTerms_stem", lower _))
      .filter($"matchesKey".isNotNull and size($"matchesKey") > 0)
      .withColumn("terms", concat(array($"keywordId"), $"matchesKey"))

    val MTN = matchesTextDF.transform(normaliseSentence(_, pipeline, "matchesTextTerms", columnsToInclude))
      .drop("text")
      .withColumn("matchesTextKey", transform($"matchesTextTerms_stem", lower _))
      .filter($"matchesTextKey".isNotNull and size($"matchesTextKey") > 0)
      .withColumnRenamed("matchesTextKey", "terms")

    val LN = labelsDF
      .transform(generateUniqueLabels)
      .selectExpr("label as text")
      .transform(normaliseSentence(_, pipeline, "labelsTerms", columnsToInclude))
      .drop("text")
      .withColumn("labelsKey", transform($"labelsTerms_stem", lower _))
      .filter($"labelsKey".isNotNull and size($"labelsKey") > 0)

    val LTN = labelsDF
      .transform(generateUniqueTexts)
      .transform(normaliseSentence(_, pipeline, "labelsTextTerms", columnsToInclude))
      .drop("text")
      .withColumn("labelsTextKey", transform($"labelsTextTerms_stem", lower _))
      .filter($"labelsTextKey".isNotNull and size($"labelsTextKey") > 0)
      .withColumnRenamed("labelsTextKey", "terms")

    LN.write.parquet(s"$output/unique_labels")

    val terms = DN.selectExpr("terms")
      .unionByName(MN.selectExpr("terms"))
      .unionByName(MTN.selectExpr("terms"))
      .unionByName(LTN.selectExpr("terms")).persist(StorageLevel.DISK_ONLY)

    terms.show(numRows = 100, truncate = 0, vertical = true)

    val w2v = new Word2Vec()
      .setWindowSize(5)
      .setVectorSize(100)
      .setNumPartitions(32)
      .setMaxIter(1)
      .setMinCount(3)
      .setStepSize(0.025)
      .setInputCol("terms")
      .setOutputCol("predictions")

    val w2vModel = w2v.fit(terms)

    w2vModel.save(s"${output}/Model")
    w2vModel.getVectors
      .withColumn("vector", vector_to_array($"vector"))
      .write.json(s"${output}/ModelVectors")

  }
}

@main
def main(prefix: String,
         labels: String,
         matches: String,
         output: String): Unit =
  ETL(prefix, labels, matches, output)
