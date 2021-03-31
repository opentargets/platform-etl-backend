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
import $ivy.`com.johnsnowlabs.nlp:spark-nlp_2.12:3.0.0`
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
import com.johnsnowlabs.nlp.{DocumentAssembler, Finisher, SparkNLP}

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
  val applyModelFn = (BU: Broadcast[Map[String, Seq[Double]]], sentence1: Seq[String], sentence2: Seq[String], cutoff: Double) => {
    val words = (sentence1 ++ sentence2).distinct.sorted
    val U = BU.value
    val W = m(Array(words.map(w => U(w).toArray):_*))
    val S1 = m(Array(sentence1.sorted.map(w => U(w).toArray):_*))
    val S2 = m(Array(sentence2.sorted.map(w => U(w).toArray):_*))

    val SS1 = zeros(S1.nrows, W.nrows)
    for {
      i <- 0 until S1.nrows
      j <- 0 until W.nrows
    } {
      SS1(i, j) = MathEx.cos(S1.row(i), W.row(j))
    }

    val SS2 = zeros(S2.nrows, W.nrows)
    for {
      i <- 0 until S2.nrows
      j <- 0 until W.nrows
    } {
      val sc = MathEx.round(MathEx.cos(S2.row(i), W.row(j)), 2)
      SS2(i, j) = if (sc > cutoff) sc else 0d
    }

    val mpS1 = MathEx.colMax(SS1.replaceNaN(0D).toArray)
    val mpS2 = MathEx.colMax(SS2.replaceNaN(0D).toArray)
    val SS = m(mpS1, mpS2)

    val minS = MathEx.colMin(SS.toArray)
    val maxS = MathEx.colMax(SS.toArray)
    MathEx.round(minS.sum / maxS.sum, 2)
  }

  val applyModelWithPOSFn = (BU: Broadcast[Map[String, Seq[Double]]],
                             sentence1: Seq[(String, String)],
                             sentence2: Seq[(String, String)], cutoff: Double) => {
    val words = sentence1 ++ sentence2
    val U = BU.value
    val W = m(Array(words.map(w => U(w._1).toArray):_*))
    val S1 = m(Array(sentence1.map(w => U(w._1).toArray):_*))
    val S2 = m(Array(sentence2.map(w => U(w._1).toArray):_*))
    val S1w = sentence1.map(_._2)
    val S2w = sentence2.map(_._2)
    val Ww = words.map(_._2)

    val SS1 = zeros(S1.nrows, W.nrows)
    for {
      i <- 0 until S1.nrows
      j <- 0 until W.nrows
    } {
      val mask = if (S1w(i) == Ww(j)) 1d else 0d
      val sc = MathEx.cos(S1.row(i), W.row(j)) * mask
      SS1(i, j) = if (sc > cutoff + MathEx.EPSILON) sc else 0d
    }

    val SS2 = zeros(S2.nrows, W.nrows)
    for {
      i <- 0 until S2.nrows
      j <- 0 until W.nrows
    } {
      val mask = if (S2w(i) == Ww(j)) 1d else 0d
      val sc = MathEx.cos(S2.row(i), W.row(j)) * mask

      SS2(i, j) = if (sc > cutoff + MathEx.EPSILON) sc else 0d
    }

    val mpS1 = MathEx.colMax(SS1.replaceNaN(0D).toArray)
    val mpS2 = MathEx.colMax(SS2.replaceNaN(0D).toArray)
    val SS = m(mpS1, mpS2)

    val minS = MathEx.colMin(SS.toArray).sum
    val maxS = MathEx.colMax(SS.toArray).sum

    if (maxS == 0d) 0d else MathEx.round(minS / maxS, 2)
  }

  val columnsToInclude = List(
    "document",
//    "token",
//    "norm",
//    "clean",
    "stem",
//    "lemm",
    "pos"
  )

  private def generatePipeline(fromCol: String, columns: List[String]): Pipeline = {
    val documentAssembler = new DocumentAssembler()
      .setInputCol(fromCol)
      .setOutputCol("document")

    val tokenizer = new Tokenizer()
      .setInputCols("document")
      .setOutputCol("token")

    val normaliser = new Normalizer()
      .setInputCols("token")
      .setOutputCol("norm")
      .setLowercase(true)
      .setCleanupPatterns(Array("[^\\w\\d\\s]"))

//    val fullTokensCleaner = new StopWordsCleaner()
//      .setInputCols("norm")
//      .setOutputCol("_clean")
//      .setStopWords(Array("disease", "disorder"))
//      .setCaseSensitive(false)

    val cleaner = StopWordsCleaner.pretrained()
      .setCaseSensitive(false)
      .setInputCols("norm")
      .setOutputCol("clean")

    val stemmer = new Stemmer()
      .setInputCols("clean")
      .setOutputCol("stem")

    val lemmatizer = LemmatizerModel.pretrained()
      .setInputCols("clean")
      .setOutputCol("lemm")

    val posTagger = PerceptronModel.pretrained("pos_ud_ewt", "en")
      .setInputCols("document", "lemm")
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
          tokenizer,
          normaliser,
//          fullTokensCleaner,
          cleaner,
          stemmer,
          lemmatizer,
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

  private def loadMeddraDf(path: String, columns: Seq[String])(
    implicit ss: SparkSession): DataFrame = {

    val meddraRaw = ss.read.csv(path)
    val meddra = meddraRaw
      .withColumn("_c0", regexp_replace(col("_c0"), "\\$+", ","))
      .withColumn("_c0", regexp_replace(col("_c0"), "\\$$", ""))
      .withColumn("_c0", split(col("_c0"), ","))
      .select(columns.zipWithIndex.map(i => col("_c0").getItem(i._2).as(s"${i._1}")): _*)

    val colsToLower = meddra.columns.filter(_.contains("name"))
    colsToLower.foldLeft(meddra)((df, c) => df.withColumn(c, lower(col(c))))

  }

  def loadMeddraPreferredTerms(path: String)(implicit ss: SparkSession): DataFrame = {
    logger.info(s"Loading Meddra preferred terms from $path")
    val cols = Seq("pt_code", "pt_name")
    loadMeddraDf(path + "/MedAscii/pt.asc", cols)
      .selectExpr("pt_code as meddraId", "pt_name as meddraName")
  }

  def loadMeddraLowLevelTerms(path: String)(implicit sparkSession: SparkSession): DataFrame = {
    logger.info(s"Loading Meddra low level terms from $path")
    val lltCols = Seq("llt_code", "llt_name")
    loadMeddraDf(path + "/MedAscii/llt.asc", lltCols)
      .selectExpr("llt_code as meddraId", "llt_name as meddraName")
  }

  def loadMeddraHighLevelTerms(path: String)(implicit sparkSession: SparkSession): DataFrame = {
    logger.info(s"Loading Meddra high level terms from $path")
    val lltCols = Seq("hlt_code", "hlt_name")
    loadMeddraDf(path + "/MedAscii/hlt.asc", lltCols)
      .selectExpr("hlt_code as meddraId", "hlt_name as meddraName")
  }

  def apply(cosineCutoff: Double, scoreCutoff: Double, prefix: String, meddra: String, matches: String, output: String) = {
    import SparkSessionWrapper._
    import spark.implicits._
    implicit val ss: SparkSession = spark

    logger.info("load required datasets from ETL parquet format")
    val diseases = spark.read.parquet(s"${prefix}/diseases")
    val meddraPT = loadMeddraPreferredTerms(meddra)
    val meddraLT = loadMeddraLowLevelTerms(meddra)
    val meddraHT = loadMeddraHighLevelTerms(meddra)

    val pipeline = generatePipeline("text", columnsToInclude)

    val D = diseases
      .selectExpr("id", "name as efoName", "synonyms.*")
      .withColumn("broadSynonyms", expr("coalesce(hasBroadSynonym, array())"))
      .withColumn("exactSynonyms", expr("coalesce(hasExactSynonym, array())"))
      .withColumn("relatedSynonyms", expr("coalesce(hasRelatedSynonym, array())"))
      .withColumn("text",
        explode(flatten(array(array($"efoName"), $"broadSynonyms", $"exactSynonyms", $"relatedSynonyms"))))
      .filter($"text".isNotNull and length($"text") > 0)

    val M = meddraPT
      .unionByName(meddraLT).unionByName(meddraHT)
      .groupBy($"meddraName")
      .agg(collect_set($"meddraId").as("meddraIds"))
      .withColumn("text", $"meddraName")

    val DN = D.transform(normaliseSentence(_, pipeline, "efoTerms", columnsToInclude))
      .drop("text")
      .withColumn("efoKey", array_sort(array_distinct($"efoTerms_stem")))
      .filter($"efoKey".isNotNull and size($"efoKey") > 0)
      .orderBy($"efoKey".asc)
      .persist(StorageLevel.DISK_ONLY)

    val MN = M.transform(normaliseSentence(_, pipeline, "meddraTerms", columnsToInclude))
      .drop("text")
      .withColumn("meddraKey", array_sort(array_distinct($"meddraTerms_stem")))
      .filter($"meddraKey".isNotNull and size($"meddraKey") > 0)
      .orderBy($"meddraKey".asc)
      .persist(StorageLevel.DISK_ONLY)

    DN.write.json(s"${output}/DiseaseLabels")
    MN.write.json(s"${output}/MeddraLabels")

//    val terms = DN.selectExpr("efoKey as terms")
//      .unionByName(MN.selectExpr("meddraKey as terms")).persist()

    val terms = DN.selectExpr("efoKey as terms").persist()

    // use matches to build the w2v model

    val w2v = new Word2Vec()
      .setWindowSize(5)
      .setVectorSize(100)
      .setNumPartitions(16)
      .setMaxIter(3)
      .setMinCount(0)
      .setStepSize(0.025)
      .setInputCol("terms")
      .setOutputCol("predictions")

    val w2vModel = w2v.fit(terms)

    w2vModel.save(s"${output}/DiseaseMeddraModel")
    w2vModel.getVectors
      .withColumn("vector", vector_to_array($"vector"))
      .write.json(s"${output}/DiseaseMeddraVectors")

    val w2vm = Word2VecModel.load(s"${output}/DiseaseMeddraModel")

    val U = w2vm.getVectors
      .withColumn("vector", vector_to_array($"vector"))
      .collect()
      .map(r => r.getAs[String]("word") -> r.getSeq[Double](1)).toMap
      .withDefaultValue(Seq.fill(100)(0d))
    val BU = spark.sparkContext.broadcast(U)
    val dynaMaxPOSFn = udf(applyModelWithPOSFn(BU, _, _, _))

    val eqLabels = MN
      .join(DN, $"meddraKey" === $"efoKey")
      .withColumn("score", lit(1D))
      .persist(StorageLevel.DISK_ONLY)

    eqLabels.write.json(s"${output}/directJoin")

    val scoreCN = "score"
    val scoreC = col(scoreCN)

    val w = Window.partitionBy($"meddraKey").orderBy(scoreC.desc, $"intersectSize".desc)
    val simLabels = MN
      .join(eqLabels.select("meddraKey"), Seq("meddraKey"), "left_anti")
      .crossJoin(DN)
      .withColumn("meddraTermsSize", size($"meddraKey"))
      .withColumn("efoTermsSize", size($"efoKey"))
      .withColumn("intersectSize", size(array_intersect($"meddraKey", $"efoKey")))
      .withColumn("unionSize", size(array_union($"meddraKey", $"efoKey")))
      .withColumn("meddraT", array_sort(arrays_zip($"meddraTerms_stem", $"meddraTerms_pos")))
      .withColumn("efoT", array_sort(arrays_zip($"efoTerms_stem", $"efoTerms_pos")))
      .filter($"intersectSize" >= 2 and $"meddraTermsSize" > 2 and $"intersectSize" >= functions.floor($"unionSize" * 1/2))
      .withColumn(scoreCN, dynaMaxPOSFn($"meddraT", $"efoT", lit(cosineCutoff)))
      .filter(scoreC > scoreCutoff + MathEx.EPSILON)
      .withColumn("rank", row_number().over(w))
      .filter($"rank" === 1)
      .drop("rank")
      .orderBy($"meddraName".asc, $"score".desc)
//      .filter(($"unionSize" === 2 and $"intersectSize" === 0 and $"scorePOS" >= 0.75) or
//        ($"unionSize" > 1 and $"intersectSize" > 0 and $"scorePOS" > scoreCutoff + MathEx.EPSILON))

    simLabels.write.json(s"${output}/crossJoin")
  }
}

@main
def main(cosineCutoff: Double = 0.5,
         scoreCutoff: Double = 0.501,
         prefix: String,
         meddra: String,
         matches: String,
         output: String): Unit =
  ETL(cosineCutoff, scoreCutoff, prefix, meddra, matches, output)
