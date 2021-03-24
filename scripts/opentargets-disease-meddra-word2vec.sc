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
//import $ivy.`org.bytedeco:javacpp:1.5.3`
//import $ivy.`org.bytedeco:openblas:0.3.9-1.5.3`
//import $ivy.`org.bytedeco:arpack-ng:3.7.0-1.5.3`
import $ivy.`com.github.haifengl:smile-mkl:2.6.0`
import $ivy.`com.github.haifengl::smile-scala:2.6.0`
import org.apache.spark.broadcast._
import org.apache.spark.ml.feature._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import org.apache.spark.ml.fpm._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.ml.functions._
import smile.math._
import smile.math.matrix.{matrix => m}
import smile.nlp._

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
  val applyModelFn = (BU: Broadcast[Map[String, Seq[Double]]], sentence1: Seq[String], sentence2: Seq[String]) => {
    val words = sentence1 ++ sentence2
    val U = BU.value
    val W = m(Array(words.map(w => U(w).toArray):_*))
    val S1 = m(Array(sentence1.map(w => U(w).toArray):_*))
    val S2 = m(Array(sentence2.map(w => U(w).toArray):_*))

    val mpS1 = MathEx.colMax((S1 %*% W.t).toArray)
    val mpS2 = MathEx.colMax((S2 %*% W.t).toArray)
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

    val diseaseLabels = diseases
      .selectExpr("id as efoId", "name as efoName")
      .withColumn("efoTerms", normaliseFn($"efoName"))
      .filter($"efoTerms".isNotNull and size($"efoTerms") > 0)

    meddraLabels.write.json(s"${output}/MeddraLabels")
    diseaseLabels.write.json(s"${output}/DiseaseLabels")

    meddraLabels.crossJoin(diseaseLabels)
      .withColumn("score", dynaMaxFn($"meddraTerms", $"efoTerms"))
      .withColumn("scoredEfo", struct($"score", $"efoId"))
      .groupBy($"meddraName")
      .agg(first($"meddraIds").as("meddraIds"),
        max($"scoredEfo").as("scoredEfo"))
      .write.json(s"${output}/crossJoin")
  }
}

@main
def main(prefix: String, meddra: String, output: String): Unit =
  ETL(prefix, meddra, output)
