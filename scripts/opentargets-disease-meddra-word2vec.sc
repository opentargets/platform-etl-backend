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
import org.apache.spark.ml.feature._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel
import org.apache.spark.ml.fpm._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.ml.functions._
import smile.math._
import smile.math.matrix.{matrix => m}
import smile.nlp._

import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import com.johnsnowlabs.nlp.SparkNLP

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
    val words = (sentence1 ++ sentence2).distinct
    val U = BU.value
    val W = m(Array(words.map(w => U(w._1).toArray):_*))
    val S1 = m(Array(sentence1.sorted.map(w => U(w._1).toArray):_*))
    val S2 = m(Array(sentence2.sorted.map(w => U(w._1).toArray):_*))
    val S1w = sentence1.sorted.map(_._2)
    val S2w = sentence2.sorted.map(_._2)
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

    val minS = MathEx.colMin(SS.toArray)
    val maxS = MathEx.colMax(SS.toArray)
    MathEx.round(minS.sum / maxS.sum, 2)
  }

  val nlpWithPOSFn = (c: Column) => udf((sentence: Option[String]) => {
    val dis = lancaster("disease")
    val words = sentence.map(s => s.normalize.words("comprehensive")).getOrElse(Array.empty)
    val w = words.zip(postag(words).map(_.name()))

    w.withFilter(_._1.nonEmpty)
      .map {
      case (word, p) => (lancaster(word).toLowerCase, p)
    }.distinct
      .filter(t => t._1.nonEmpty && t._1 != dis)
      .sortBy(_._1)
  }).apply(c)

  val nlpFn = (c: Column) => udf((sentence: Option[String]) => {
    val dis = lancaster("disease")
    sentence.map(s => s.normalize.words("comprehensive")
      .withFilter(_.nonEmpty)
      .map(w => {
        lancaster(w).toLowerCase
      })
      .distinct
      .filter(t => t.nonEmpty && t != dis)
      .sorted
    )
  }).apply(c)

  val translateFn = (c: Column) => translate(c, "-", " ")
  val normaliseFn = nlpFn compose translateFn
  val normaliseWithPOSFn = nlpWithPOSFn compose translateFn

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
    logger.info(s"Loading Meddra low level terms from $path")
    val lltCols = Seq("hlt_code", "hlt_name")
    loadMeddraDf(path + "/MedAscii/hlt.asc", lltCols)
      .selectExpr("hlt_code as meddraId", "hlt_name as meddraName")
  }

  def apply(cosineCutoff: Double, scoreCutoff: Double, prefix: String, meddra: String, output: String) = {
    import SparkSessionWrapper._
    import spark.implicits._
    implicit val ss: SparkSession = spark

    logger.info("load required datasets from ETL parquet format")
    val diseases = spark.read.parquet(s"${prefix}/diseases")
//    val diseasehp = spark.read.parquet(s"${prefix}/disease_hpo")
//    val hp = spark.read.parquet(s"${prefix}/hpo")
    val meddraPT = loadMeddraPreferredTerms(meddra)
    val meddraLT = loadMeddraLowLevelTerms(meddra)
    val meddraHT = loadMeddraHighLevelTerms(meddra)

    val D = diseases
      .selectExpr("id", "name as efoName", "synonyms.*")
      .withColumn("broadSynonyms", expr("coalesce(hasBroadSynonym, array())"))
      .withColumn("exactSynonyms", expr("coalesce(hasExactSynonym, array())"))
      .withColumn("relatedSynonyms", expr("coalesce(hasRelatedSynonym, array())"))
      .withColumn("synonym", explode(flatten(array(array($"efoName"), $"broadSynonyms", $"exactSynonyms", $"relatedSynonyms"))))
      .filter($"synonym".isNotNull and length($"synonym") > 0)
      .withColumn("efoNameN", normaliseFn($"efoName"))
      .withColumn("synonymN", normaliseFn($"synonym"))
      .withColumn("terms", array_sort(array_distinct(concat($"efoNameN", $"synonymN"))))
      .selectExpr("terms")

    val MDTerms = meddraPT
      .unionByName(meddraLT).unionByName(meddraHT)
      .select($"meddraName")
      .distinct()
      .withColumn("terms", normaliseFn($"meddraName"))
      .selectExpr("terms")

    val terms = D.unionByName(MDTerms).persist()


    val w2v = new Word2Vec()
      .setWindowSize(5)
      .setNumPartitions(16)
      .setMaxIter(1)
      .setMinCount(0)
      .setStepSize(0.025)
      .setInputCol("terms")
      .setOutputCol("predictions")

    val w2vModel = w2v.fit(terms)

    terms.write.json(s"${output}/DiseaseMeddraTerms")
    w2vModel.save(s"${output}/DiseaseMeddraModel")
    w2vModel.getVectors
      .withColumn("vector", vector_to_array($"vector"))
      .write.json(s"${output}/DiseaseMeddraVectors")

    val w2vm = Word2VecModel.load(s"${output}/DiseaseMeddraModel")


    val U = w2vm.getVectors
      .withColumn("vector", vector_to_array($"vector"))
      .collect()
      .map(r => r.getAs[String]("word") -> r.getSeq[Double](1)).toMap
    val BU = spark.sparkContext.broadcast(U)
    val dynaMaxFn = udf(applyModelFn(BU, _, _, _))
    val dynaMaxPOSFn = udf(applyModelWithPOSFn(BU, _, _, _))

    val meddraLabels = meddraPT
      .unionByName(meddraLT)
      .groupBy($"meddraName")
      .agg(collect_set($"meddraId").as("meddraIds"))
      .withColumn("meddraTerms", normaliseFn($"meddraName"))
      .withColumn("meddraTermsPOS", normaliseWithPOSFn($"meddraName"))
      .filter($"meddraTerms".isNotNull and size($"meddraTerms") > 0)
      .orderBy($"meddraTerms".asc)
      .persist()

    val diseaseLabels = diseases
      .selectExpr("id as efoId", "name", "synonyms.*")
      .withColumn("broadSynonyms", expr("coalesce(hasBroadSynonym, array())"))
      .withColumn("exactSynonyms", expr("coalesce(hasExactSynonym, array())"))
      .withColumn("relatedSynonyms", expr("coalesce(hasRelatedSynonym, array())"))
      .withColumn("synonym", explode(flatten(array(array($"name"), $"broadSynonyms", $"exactSynonyms", $"relatedSynonyms"))))
      .filter($"synonym".isNotNull and length($"synonym") > 0)
      .drop("name")
      .withColumnRenamed("synonym", "efoName")
      .withColumn("efoTerms", normaliseFn($"efoName"))
      .withColumn("efoTermsPOS", normaliseWithPOSFn($"efoName"))
      .filter($"efoTerms".isNotNull and size($"efoTerms") > 0)
      .orderBy($"efoTerms".asc)
      .persist()

    val pipeline = new PretrainedPipeline("explain_document_dl", lang = "en")
    val annotations = pipeline
      .transform(diseaseLabels.withColumnRenamed("efoName", "text"))

    annotations.write.json(s"${output}/DiseaseLabelsAnnotated")

    meddraLabels.write.json(s"${output}/MeddraLabels")
    diseaseLabels.write.json(s"${output}/DiseaseLabels")

    val eqLabels = meddraLabels
      .withColumn("meddraKey", concat($"meddraTerms"))
      .join(diseaseLabels.withColumn("efoKey", concat($"efoTerms")), $"meddraKey" === $"efoKey")
      .withColumn("score", lit(1D))
      .selectExpr("meddraName", "meddraIds", "efoId", "efoName", "score")
      .persist(StorageLevel.DISK_ONLY)

    eqLabels.write.json(s"${output}/directJoin")

    val colNames = Seq(
      "meddraName",
      "meddraIds",
      "efoId",
      "efoName",
      "scorePOS as score",
//      "meddraTerms",
      "meddraTermsPOS",
//      "efoTerms",
      "efoTermsPOS",
      "intersectSize",
      "unionSize",
      "meddraTermsSize",
      "efoTermsSize"
    )
    val w = Window.partitionBy($"meddraName").orderBy($"intersectSize".desc, $"scorePOS".desc)
    val simLabels = meddraLabels
      .join(eqLabels.select("meddraName"), Seq("meddraName"), "left_anti")
      .crossJoin(diseaseLabels)
      .withColumn("meddraTermsSize", size($"meddraTerms"))
      .withColumn("efoTermsSize", size($"efoTerms"))
      .withColumn("intersectSize", size(array_intersect($"meddraTerms", $"efoTerms")))
      .withColumn("unionSize", size(array_union($"meddraTerms", $"efoTerms")))
    // (.intersectSize >= 2 and  .meddraTermsSize > 2)
      .filter($"intersectSize" >= 2 and $"meddraTermsSize" > 2 and $"intersectSize" >= functions.floor($"unionSize" * 1/2))
//      .withColumn("scoreNoPOS", dynaMaxFn($"meddraTerms", $"efoTerms", lit(cosineCutoff)))
      .withColumn("scorePOS", dynaMaxPOSFn($"meddraTermsPOS", $"efoTermsPOS", lit(cosineCutoff)))
      .filter($"scorePOS" > scoreCutoff + MathEx.EPSILON)
      .withColumn("rank", row_number().over(w))
      .filter($"rank" === 1)
      .orderBy($"meddraName".asc, $"intersectSize".desc, $"scorePOS".desc)
      .selectExpr(colNames:_*)
//      .filter(($"unionSize" === 2 and $"intersectSize" === 0 and $"scorePOS" >= 0.75) or
//        ($"unionSize" > 1 and $"intersectSize" > 0 and $"scorePOS" > scoreCutoff + MathEx.EPSILON))

    simLabels.write.json(s"${output}/crossJoin")
  }
}

@main
def main(cosineCutoff: Double = 0.5, scoreCutoff: Double = 0.501, prefix: String, meddra: String, output: String): Unit =
  ETL(cosineCutoff, scoreCutoff, prefix, meddra, output)
