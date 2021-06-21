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

  val applyModelWithPOSFn = (BU: Broadcast[Map[String, Seq[Double]]],
                             sentence1: Seq[(String, String)],
                             sentence2: Seq[(String, String)]) => {
    val words = sentence1 ++ sentence2
    val U = BU.value
    val W = m(Array(words.map(w => U(w._1).toArray):_*))
    val S1 = m(Array(sentence1.map(w => U(w._1).toArray):_*))
    val S2 = m(Array(sentence2.map(w => U(w._1).toArray):_*))

    val SS1 = zeros(S1.nrows, W.nrows)
    for {
      i <- 0 until S1.nrows
      j <- 0 until W.nrows
    } {
      val sc = MathEx.cos(S1.row(i), W.row(j))
      SS1(i, j) = sc
    }

    val SS2 = zeros(S2.nrows, W.nrows)
    for {
      i <- 0 until S2.nrows
      j <- 0 until W.nrows
    } {
      val sc = MathEx.cos(S2.row(i), W.row(j))
      SS2(i, j) = sc
    }

    val mpS1 = MathEx.colMax(SS1.replaceNaN(0D).toArray)
    val mpS2 = MathEx.colMax(SS2.replaceNaN(0D).toArray)
    val SS = m(mpS1, mpS2)

    val minS = MathEx.colMin(SS.toArray).sum
    val maxS = MathEx.colMax(SS.toArray).sum

    if (maxS == 0d) 0d else minS / maxS
  }

  val columnsToInclude = List(
    "document",
    "token",
    "stop",
    "clean",
    "stem",
//    "lemm",
    "pos"
  )

  private def generatePipeline(fromCol: String, columns: List[String]): Pipeline = {
    // https://nlp.johnsnowlabs.com/docs/en/models#english---models
    val documentAssembler = new DocumentAssembler()
      .setInputCol(fromCol)
      .setOutputCol("document")

    val tokenizer = new Tokenizer()
      .setSplitChars(Array("-", "/", ":", ",", ";"))
      .setInputCols("document")
      .setOutputCol("token")

//    val fullTokensCleaner = new StopWordsCleaner()
//      .setInputCols("norm")
//      .setOutputCol("_clean")
//      .setStopWords(Array("disease", "disorder"))
//      .setCaseSensitive(false)

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

//    val lemmatizer = LemmatizerModel.pretrained()
//      .setInputCols("clean")
//      .setOutputCol("lemm")

//    val posTagger = PerceptronModel.pretrained("pos_ud_ewt", "en")
    val posTagger = PerceptronModel.pretrained("pos_anc", "en")
      .setInputCols("document", "clean")
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
          cleaner,
          normaliser,
//          fullTokensCleaner,
          stemmer,
//          lemmatizer,
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
    val cols = Seq("traitCode", "traitName")
    loadMeddraDf(path + "/MedAscii/pt.asc", cols)
      .selectExpr(cols:_*)
  }

  def loadMeddraLowLevelTerms(path: String)(implicit sparkSession: SparkSession): DataFrame = {
    logger.info(s"Loading Meddra low level terms from $path")
    val cols = Seq("traitCode", "traitName")
    loadMeddraDf(path + "/MedAscii/llt.asc", cols)
      .selectExpr(cols:_*)
  }

  def loadMeddraHighLevelTerms(path: String)(implicit sparkSession: SparkSession): DataFrame = {
    logger.info(s"Loading Meddra high level terms from $path")
    val cols = Seq("traitCode", "traitName")
    loadMeddraDf(path + "/MedAscii/hlt.asc", cols)
      .selectExpr(cols:_*)
  }

  def loadTraitsGenetics(path: String)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    val options = Map(
      "sep" -> ",",
      "inferSchema" -> "true",
      "header" -> "false"
    )

    val traits = sparkSession.read.options(options).csv(path)

    traits.toDF("studyId", "traitName", "currentEfo")
      .selectExpr(
        "traitName",
        "struct('studyId', studyId, 'currentEfo', currentEfo) as traitId"
      )
  }

  def loadTraitsJSON(path: String)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    val traits = sparkSession.read.json(path)
    traits.toDF("traitCode", "traitName")
      .groupBy($"traitName")
      .agg(collect_set($"traitCode").as("traitId"))
  }

  def loadMeddraTraits(path: String)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    val meddraPT = loadMeddraPreferredTerms(path)
    val meddraLT = loadMeddraLowLevelTerms(path)
    val meddraHT = loadMeddraHighLevelTerms(path)
    val M = meddraPT
      .unionByName(meddraLT).unionByName(meddraHT)
      .groupBy($"traitName")
      .agg(collect_set($"traitCode").as("traitIds"))
      .withColumn("traitId", array_join($"traitIds", "-"))
    M
  }

  def apply(cutoff: Double, prefix: String, traitsPath: String, matches: String, output: String) = {
    import SparkSessionWrapper._
    import spark.implicits._
    implicit val ss: SparkSession = spark

    logger.info("load required datasets from ETL parquet format")
    val diseases = spark.read.parquet(s"${prefix}/diseases")

    // val traits = loadMeddraTraits(traitsPath)
    val traits = loadTraitsGenetics(traitsPath)
    // val traits = loadMeddraTraits(traitsPath)
    // val traits = loadTraits(traitsPath)
    // val traits = loadTraitsJSON(traitsPath)
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

    val DN = D.transform(normaliseSentence(_, pipeline, "efoTerms", columnsToInclude))
      .drop("text")
      .withColumn("efoKey", transform(array_sort(array_distinct($"efoTerms_stem")), lower _))
      .filter($"efoKey".isNotNull and size($"efoKey") > 0)
      .withColumn("efoT", array_sort(arrays_zip($"efoTerms_stem", $"efoTerms_pos")))
      .withColumn("efoTNN", transform(filter($"efoT", _.getField("efoTerms_pos").startsWith("NN")), _.getField("efoTerms_stem")))
      .withColumn("efoTJJ", transform(filter($"efoT", _.getField("efoTerms_pos").startsWith("JJ")), _.getField("efoTerms_stem")))
      .orderBy($"efoKey".asc)
      .persist(StorageLevel.DISK_ONLY)

    val M = traits.withColumn("text", $"traitName")

    val MN = M.transform(normaliseSentence(_, pipeline, "traitTerms", columnsToInclude))
      .drop("text")
      .withColumn("traitKey", transform(array_sort(array_distinct($"traitTerms_stem")), lower _))
      .filter($"traitKey".isNotNull and size($"traitKey") > 0)
      .withColumn("traitT", array_sort(arrays_zip($"traitTerms_stem", $"traitTerms_pos")))
      .withColumn("traitTNN", transform(filter($"traitT", _.getField("traitTerms_pos").startsWith("NN")), _.getField("traitTerms_stem")))
      .withColumn("traitTJJ", transform(filter($"traitT", _.getField("traitTerms_pos").startsWith("JJ")), _.getField("traitTerms_stem")))
      .orderBy($"traitKey".asc)
      .persist(StorageLevel.DISK_ONLY)

//    DN.write.json(s"${output}/DiseaseLabels")
//    MN.write.json(s"${output}/TraitLabels")

//    val terms = DN.selectExpr("efoKey as terms")
//      .unionByName(MN.selectExpr("traitKey as terms")).persist()

//    val w2v = new Word2Vec()
//      .setWindowSize(5)
//      .setVectorSize(100)
//      .setNumPartitions(16)
//      .setMaxIter(3)
//      .setMinCount(0)
//      .setStepSize(0.025)
//      .setInputCol("terms")
//      .setOutputCol("predictions")
//
//    val w2vModel = w2v.fit(terms)
//
//    w2vModel.save(s"${output}/Model")
//    w2vModel.getVectors
//      .withColumn("vector", vector_to_array($"vector"))
//      .write.json(s"${output}/ModelVectors")
//
//    val w2vm = Word2VecModel.load(s"${output}/Model")

    val w2vm = Word2VecModel.load(matches)

    val U = w2vm.getVectors
      .withColumn("vector", vector_to_array($"vector"))
      .collect()
      .map(r => r.getAs[String]("word") -> r.getSeq[Double](1)).toMap
      .withDefaultValue(Seq.fill(100)(0d))
    val BU = spark.sparkContext.broadcast(U)
    val dynaMaxPOSFn = udf(applyModelWithPOSFn(BU, _, _))

    val scoreCN = "score"
    val scoreC = col(scoreCN)

    val eqW = Window.partitionBy($"traitId", $"efoId").orderBy(scoreC.desc)
    val eqLabels = MN
      .join(DN, $"traitKey" === $"efoKey")
      .withColumn("score", lit(1D) * $"factor")
      .withColumn("rank", dense_rank().over(eqW))
      .filter($"rank" === 1)
      .persist(StorageLevel.DISK_ONLY)

    eqLabels.write.json(s"${output}/directJoin")

    val xqW = Window.partitionBy($"traitId", $"efoId").orderBy($"intersectSizeNN".desc, $"intersectSizeJJ".desc, $"intersectSize".desc, scoreC.desc)
    val w = Window.partitionBy($"traitId").orderBy(scoreC.desc)
    val simLabels = MN
      .crossJoin(DN)
      .withColumn("traitTermsSize", size($"traitKey"))
      .withColumn("efoTermsSize", size($"efoKey"))
      .withColumn("intersectSize", size(array_intersect($"traitKey", $"efoKey")))
      .withColumn("unionSize", size(array_union($"traitKey", $"efoKey")))
      .withColumn("intersectSizeNN", size(array_intersect(
        $"traitTNN",
        $"efoTNN",
      )))
      .withColumn("intersectSizeJJ", size(array_intersect(
        $"traitTJJ",
        $"efoTJJ",
      )))
      .filter($"intersectSizeNN" >= 1 and $"intersectSizeJJ" >= 0 and $"traitTermsSize" >= 2)
      .withColumn(scoreCN, dynaMaxPOSFn($"traitT", $"efoT") * lit(0.995)) // keep all under direct
      .withColumn("rank", dense_rank().over(xqW))
      .filter($"rank" === 1)
      .drop("rank")
      .orderBy($"traitName".asc, scoreC.desc)
      .persist(StorageLevel.DISK_ONLY)

    simLabels.write.json(s"${output}/crossJoin")

    val cols = Seq(
      "traitId",
      "traitName",
      "score",
      "efoId",
      "efoName"
    )

    eqLabels.selectExpr(cols:_*).unionByName(simLabels.selectExpr(cols:_*))
      .withColumn("rank", dense_rank().over(w))
      .filter($"rank" === 1)
      .drop("rank")
      .dropDuplicates
      .write.json(s"${output}/fullJoin")
  }
}

@main
def main(cutoff: Double = 0.5,
         prefix: String,
         traits: String,
         matches: String,
         output: String): Unit =
  ETL(cutoff, prefix, traits, matches, output)
