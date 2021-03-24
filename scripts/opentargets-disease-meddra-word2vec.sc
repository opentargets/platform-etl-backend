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

  val applyModelFn = (BU: Broadcast[Map[String, Seq[Double]]], sentence1: Seq[String], sentence2: Seq[String]) => {
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
      SS2(i, j) = MathEx.cos(S2.row(i), W.row(j))
    }

    val mpS1 = MathEx.colMax(SS1.replaceNaN(0D).toArray)
    val mpS2 = MathEx.colMax(SS2.replaceNaN(0D).toArray)
    val SS = m(mpS1, mpS2)

    val minS = MathEx.colMin(SS.toArray)
    val maxS = MathEx.colMax(SS.toArray)
    minS.sum / maxS.sum
  }

  def normalise(c: Column): Column = {
    // https://www.rapidtables.com/math/symbols/greek_alphabet.html
    sort_array(filter(split(lower(translate(
      rtrim(lower(translate(trim(trim(c), "."), "/`''[]{}()-,", "")), "s"),
      "αβγδεζηικλμνξπτυω",
      "abgdezhiklmnxptuo"
    )), " "), d => length(d) > 2), asc = true)
  }

  val nlpFn = (c: Column) => udf((sentence: Option[String]) =>
    sentence.map(s => s.normalize.words().map(w => lancaster(w).toLowerCase).distinct.filter(_.nonEmpty).sorted)).apply(c)
  val translateFn = (c: Column) => translate(c, "-", " ")
  val normaliseFn = nlpFn compose translateFn

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

  def apply(prefix: String, meddra: String, output: String) = {
    import SparkSessionWrapper._
    import spark.implicits._
    implicit val ss: SparkSession = spark

    logger.info("load required datasets from ETL parquet format")
    val diseases = spark.read.parquet(s"${prefix}/diseases")
    val diseasehp = spark.read.parquet(s"${prefix}/disease_hpo")
    val hp = spark.read.parquet(s"${prefix}/hpo")
    val meddraPT = loadMeddraPreferredTerms(meddra)
    val meddraLT = loadMeddraLowLevelTerms(meddra)

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
      .unionByName(meddraLT)
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
    val dynaMaxFn = udf(applyModelFn(BU, _, _))

    val meddraLabels = meddraPT
      .unionByName(meddraLT)
      .groupBy($"meddraName")
      .agg(collect_set($"meddraId").as("meddraIds"))
      .withColumn("meddraTerms", normaliseFn($"meddraName"))
      .filter($"meddraTerms".isNotNull and size($"meddraTerms") > 0)
      .orderBy($"meddraTerms".asc)
      .persist()

    val diseaseLabels = diseases
      .selectExpr("id as efoId", "name as efoName")
      .withColumn("efoTerms", normaliseFn($"efoName"))
      .filter($"efoTerms".isNotNull and size($"efoTerms") > 0)
      .orderBy($"efoTerms".asc)
      .persist()

    meddraLabels.write.json(s"${output}/MeddraLabels")
    diseaseLabels.write.json(s"${output}/DiseaseLabels")

    val eqLabels = meddraLabels
      .withColumn("meddraKey", concat($"meddraTerms"))
      .join(diseaseLabels.withColumn("efoKey", concat($"efoTerms")), $"meddraKey" === $"efoKey")
      .withColumn("score", lit(1D))
      .selectExpr("meddraName", "meddraIds", "efoId", "efoName", "score")
      .persist(StorageLevel.DISK_ONLY)

    eqLabels.write.json(s"${output}/directJoin")

    val w = Window.partitionBy($"meddraName").orderBy($"score".desc)
    val simLabels = meddraLabels
      .join(eqLabels.select("meddraName"), Seq("meddraName"), "left_anti")
      .crossJoin(diseaseLabels)
      .withColumn("intersectSize", size(array_intersect($"meddraTerms", $"efoTerms")))
      .withColumn("unionSize", size(array_union($"meddraTerms", $"efoTerms")))
      .filter($"intersectSize" >= functions.round($"unionSize" * 2/3))
      .drop("intersectSize", "unionSize")
      .withColumn("score", dynaMaxFn($"meddraTerms", $"efoTerms"))
      .filter($"score" > 0.8)
      .withColumn("rank", row_number().over(w))
      .filter($"rank" === 1)
      .selectExpr("meddraName", "meddraIds", "efoId", "efoName", "score")
      .orderBy($"efoId".asc, $"score".desc)

    simLabels.write.json(s"${output}/crossJoin")
  }
}

@main
def main(prefix: String, meddra: String, output: String): Unit =
  ETL(prefix, meddra, output)
