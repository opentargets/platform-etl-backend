// how to get the vectors, operate and find synonyms
// val vectors = model.getVectors
//  .filter($"word" isInCollection(Seq(pi3k, atk1, "ENSG00000105221", "ENSG00000140992", "ENSG00000152256")))
//  .agg(Summarizer.sum($"vector").as("v")).select("v").collect.head.getAs[Vector]("v")
// model.findSynonyms(vectors, 10).show()

import $file.resolvers
import $file.opentargetsFunctions
import $ivy.`ch.qos.logback:logback-classic:1.2.3`
import $ivy.`com.typesafe.scala-logging::scala-logging:3.9.2`
import $ivy.`com.typesafe:config:1.4.0`

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.sql.expressions.Window
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

import opentargetsFunctions.OpentargetsFunctions._

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
  val applyModelFn = (model: Broadcast[Word2VecModel], word: String) => {
    try {
      model.value.findSynonymsArray(word, 100).filter(_._2 > 0.1)
    } catch {
      case _ => Array.empty[(String, Double)]
    }
  }

  val cols = List("model", "word", "prediction", "score")
  def apply(prefix: String, output: String) = {
    import SparkSessionWrapper._
    import spark.implicits._

    logger.info("load required datasets from ETL parquet format")
    val diseases = spark.read.parquet(s"${prefix}/diseases")
    val indications = spark.read.parquet(s"${prefix}/drugs/indication")
    val diseasehp = spark.read.parquet(s"${prefix}/disease_hpo")
    val targets = spark.read.parquet(s"${prefix}/targets")
    val faers = spark.read.parquet(s"${prefix}/openfda")
    val compounds = spark.read.parquet(s"${prefix}/drugs/drug")
    val moas = spark.read.parquet(s"${prefix}/drugs/mechanism_of_action").withColumn("chemblId", explode($"chemblIds"))
    val interactions = spark.read.parquet(s"${prefix}/interactions")
      .filter($"speciesB.taxon_id" === 9606 and
        $"targetA".startsWith("ENSG") and
        $"targetB".startsWith("ENSG") and
        $"targetA" =!= $"targetB")
    val evs = spark.read.parquet(s"${prefix}/evidences/succeeded")
    val coocs = spark.read.parquet(s"${prefix}/epmc/cooccurrences")

    //+---------------+--------+
    //|geneId         |symbol  |
    //+---------------+--------+
    //|ENSG00000002586|CD99    |
    //|ENSG00000002933|TMEM176A|
    //+---------------+--------+
    val T = targets
      .selectExpr("id as geneId", "approvedSymbol as symbol")

    //+-------------+------------------------------------------+-------------+
    //|efoId        |TAs                                       |ancestor     |
    //+-------------+------------------------------------------+-------------+
    //|MONDO_0044792|[OTAR_0000018, MONDO_0045024, EFO_0010285]|MONDO_0044792|
    //|MONDO_0044792|[OTAR_0000018, MONDO_0045024, EFO_0010285]|EFO_0010285  |
    //+-------------+------------------------------------------+-------------+
    val D = diseases
      .selectExpr("id as efoId", "therapeuticAreas as TAs", "concat(array(id), ancestors) as ancestors")
      .withColumn("ancestor", explode($"ancestors"))
      .drop("ancestors")

    //+------------+-----------+
    //|chemblId    |efoId      |
    //+------------+-----------+
    //|CHEMBL100116|EFO_0003843|
    //|CHEMBL100116|EFO_0000289|
    //+------------+-----------+
    val I = indications
      .withColumn("indication", explode($"indications"))
      .selectExpr("id as chemblId", "indication.*")
      .selectExpr("chemblId", "disease as efoId")

    //+-------------+---------------+----------+
    //|chemblId     |geneId         |actionType|
    //+-------------+---------------+----------+
    //|CHEMBL3545331|ENSG00000163485|AGONIST   |
    //|CHEMBL442    |ENSG00000120907|AGONIST   |
    //+-------------+---------------+----------+
    val M = moas.withColumn("target", explode($"targets"))
      .withColumnRenamed("target", "geneId")
      .selectExpr("chemblId", "geneId", "actionType")

    //+------------+--------------+
    //|chemblId    |drugType      |
    //+------------+--------------+
    //|CHEMBL100014|Small molecule|
    //|CHEMBL10188 |Small molecule|
    //+------------+--------------+
    val C = compounds.selectExpr("id as chemblId", "drugType")
      .orderBy($"chemblId")

    //+------------+---------------+-----------+--------------+----------+-------------+
    //|chemblId    |geneId         |efoId      |drugType      |actionType|TAs          |
    //+------------+---------------+-----------+--------------+----------+-------------+
    //|CHEMBL100116|ENSG00000147955|EFO_0003765|Small molecule|MODULATOR |[EFO_0000651]|
    //|CHEMBL100116|ENSG00000147955|EFO_0000651|Small molecule|MODULATOR |[EFO_0000651]|
    //+------------+---------------+-----------+--------------+----------+-------------+
    val DR = C
      .join(I, Seq("chemblId"), "left_outer")
      .join(M, Seq("chemblId"), "left_outer")
//      .join(D,Seq("efoId"), "left_outer")
//      .drop("efoId")
//      .withColumnRenamed("ancestor", "efoId")
      .selectExpr("chemblId", "geneId", "efoId", "drugType", "actionType")

    //+---------------+---------------+-----+
    //|geneA          |geneB          |count|
    //+---------------+---------------+-----+
    //|ENSG00000146648|ENSG00000177885|110  |
    //|ENSG00000141510|ENSG00000135679|108  |
    //+---------------+---------------+-----+
    val N = interactions.selectExpr("targetA as geneA", "targetB as geneB", "count")
      .groupBy($"geneA", $"geneB")
      .agg(sum($"count").as("score"))
      .filter($"score" > 2)

    //+-----------+-----------+-----------------------------------------+-------------+
    //|efoId      |phenotypeId|TAs                                      |ancestor     |
    //+-----------+-----------+-----------------------------------------+-------------+
    //|EFO_0000182|HP_0001413 |[EFO_0010282, MONDO_0045024, EFO_0001379]|MONDO_0024276|
    //|EFO_0000182|HP_0001413 |[EFO_0010282, MONDO_0045024, EFO_0001379]|EFO_0006858  |
    //+-----------+-----------+-----------------------------------------+-------------+
    val DP = diseasehp.withColumn("ev", explode($"evidence"))
      .drop("evidence")
      .filter($"ev.qualifierNot" === false)
      .selectExpr("disease as efoId", "phenotype as phenotypeId")
      .distinct
      .join(D, Seq("efoId"))
      .drop("TAs")

    //+--------------+---------------+-----+-----------+-----------+
    //|directEfoId   |geneId         |score|vId        |efoId      |
    //+--------------+---------------+-----+-----------+-----------+
    //|Orphanet_88619|ENSG00000153201|0.32 |rs773278648|EFO_0005774|
    //|Orphanet_88619|ENSG00000153201|0.32 |rs773278648|EFO_0009386|
    //+--------------+---------------+-----+-----------+-----------+
    val E = evs.filter($"datatypeId" isInCollection(Seq("genetic_literature", "genetic_association")))
      .selectExpr("diseaseId as efoId", "targetId as geneId", "score",
        "coalesce(variantId, variantRsId, studyId) as vId")
      .join(D, Seq("efoId"))
      .drop("TAs")

    //+-------------+----------+------------------+
    //|chemblId     |meddraCode|score             |
    //+-------------+----------+------------------+
    //|CHEMBL1200343|10049460  |0.9443225592058297|
    //|CHEMBL1200343|10060921  |0.1697153897748649|
    //+-------------+----------+------------------+
    val AE = faers.selectExpr("chembl_id as chemblId", "meddraCode", "llr as score")

    //+-----------+---------------+-----------+-------------------+---------------+------------+--------------+----------+------------+
    //|efoId      |geneId         |directEfoId|score              |vId            |chemblId    |drugType      |actionType|TA          |
    //+-----------+---------------+-----------+-------------------+---------------+------------+--------------+----------+------------+
    //|EFO_0000222|ENSG00000171552|EFO_0000222|0.06565428525209427|20_31839611_G_A|CHEMBL408194|Small molecule|INHIBITOR |OTAR_0000006|
    //|EFO_0000222|ENSG00000171552|EFO_0000222|0.06565428525209427|20_31839611_G_A|CHEMBL408194|Small molecule|INHIBITOR |EFO_0005803 |
    //+-----------+---------------+-----------+-------------------+---------------+------------+--------------+----------+------------+
    logger.info("generate indirect genetic evidences+drugs aggregation")
    val EDR = E.join(DR, Seq("efoId", "geneId"))

    //+-----------+------------+----------+--------------------+
    //|      efoId|         vId|actionType|                   v|
    //+-----------+------------+----------+--------------------+
    //|EFO_0000222|rs1060502568| INHIBITOR|[EFO_0000222, CHE...|
    //|EFO_0000228|RCV001161526|ANTAGONIST|[EFO_0000228, CHE...|
    //+-----------+------------+----------+--------------------+
    logger.info("generate model for genetics aggregation and write")
    val EDRAGG = EDR.groupBy($"ancestor", $"vId", $"actionType")
      .agg(flatten(transform(sort_array(collect_set(struct($"score", $"efoId", $"chemblId")), asc=false),
        c => array(c.getField("efoId"), c.getField("chemblId")) )).as("v"))

    val EDRAGGW2V = new Word2Vec().setWindowSize(10).setNumPartitions(32).setMaxIter(5).setInputCol("v").setOutputCol("predictions")
    val EDRAGGW2VModel = EDRAGGW2V.fit(EDRAGG)
    EDRAGGW2VModel.save(s"${output}/models/EDRAGGW2VModel")

    val EDRAGGW2VModelB = spark.sparkContext.broadcast(EDRAGGW2VModel)
    val EDRAGGFn = applyModelFn(EDRAGGW2VModelB, _)

    EDRAGGW2VModel.getVectors.withColumn("predictions", udf(EDRAGGFn).apply($"word"))
      .withColumn("model", lit("GENETICS"))
      .withColumn("_prediction", explode($"predictions"))
      .withColumn("prediction", $"_prediction".getField("_1"))
      .withColumn("score", $"_prediction".getField("_2"))
      .select(cols.head, cols.tail:_*)
      .write.parquet(s"${output}/models/EDRAGGPredictions")

    logger.info("generate model for phenotypes aggregation and write")
    val DPAGG = DP.join(DR.drop("geneId").distinct(), Seq("efoId"))
      .groupBy($"phenotypeId", $"ancestor", $"drugType")
      .agg(flatten(collect_set(array($"efoId", $"chemblId"))).as("v"))

    val DPAGGW2V = new Word2Vec().setWindowSize(10).setNumPartitions(32).setMaxIter(5).setInputCol("v").setOutputCol("predictions")
    val DPAGGW2VModel = DPAGGW2V.fit(DPAGG)
    DPAGGW2VModel.save(s"${output}/models/DPAGGW2VModel")

    val DPAGGW2VModelB = spark.sparkContext.broadcast(DPAGGW2VModel)
    val DPAGGFn = applyModelFn(DPAGGW2VModelB, _)

    DPAGGW2VModel.getVectors.withColumn("predictions", udf(DPAGGFn).apply($"word"))
      .withColumn("model", lit("PHENOTYPES"))
      .withColumn("_prediction", explode($"predictions"))
      .withColumn("prediction", $"_prediction".getField("_1"))
      .withColumn("score", $"_prediction".getField("_2"))
      .select(cols.head, cols.tail:_*)
      .write.parquet(s"${output}/models/DPAGGPredictions")

    logger.info("generate model for EPMC aggregation and write")
    val EPMC = coocs.filter($"type" === "DS-CD" and $"isMapped" === true)
      .selectExpr("pmid", "evidence_score / 10.0 as score", "keywordId1 as efoId", "keywordId2 as chemblId")
      .groupBy($"pmid", $"efoId", $"chemblId").agg(mean($"score").as("score"))

    val EPMCAGG = EPMC.join(C,Seq("chemblId"))
      .groupBy($"pmId", $"drugType")
      .agg(flatten(transform(sort_array(collect_set(struct($"score", $"efoId", $"chemblId")), asc=false),
        x => array(x.getField("efoId"), x.getField("chemblId")))).as("v"))

    val EPMCAGGW2V = new Word2Vec().setWindowSize(10).setNumPartitions(32).setMaxIter(5).setInputCol("v").setOutputCol("predictions")
    val EPMCAGGW2VModel = EPMCAGGW2V.fit(EPMCAGG)
    EPMCAGGW2VModel.save(s"${output}/models/EPMCAGGW2VModel")

    val EPMCAGGW2VModelB = spark.sparkContext.broadcast(EPMCAGGW2VModel)
    val EPMCAGGFn = applyModelFn(EPMCAGGW2VModelB, _)

    EPMCAGGW2VModel.getVectors.withColumn("predictions", udf(EPMCAGGFn).apply($"word"))
      .withColumn("model", lit("EPMC-DS-DC"))
      .withColumn("_prediction", explode($"predictions"))
      .withColumn("prediction", $"_prediction".getField("_1"))
      .withColumn("score", $"_prediction".getField("_2"))
      .select(cols.head, cols.tail:_*)
      .write.parquet(s"${output}/models/EPMCAGGPredictions")

    logger.info("generate model for interactions aggregation and write")
    val INTAGG = N.join(DR.filter($"geneId".isNotNull and $"efoId".isNotNull).join(D.drop("TAs"), Seq("efoId")), $"geneB" === $"geneId" or $"geneA" === $"geneId")
      .groupBy($"geneA", $"ancestor", $"actionType", $"drugType")
      .agg(flatten(transform(sort_array(collect_set(struct($"score", $"efoId", $"chemblId")), asc=false), x => array(x.getField("efoId"), x.getField("chemblId")))).as("v"))

    val INTAGGW2V = new Word2Vec().setWindowSize(10).setNumPartitions(32).setMaxIter(5).setInputCol("v").setOutputCol("predictions")
    val INTAGGW2VModel = INTAGGW2V.fit(INTAGG)
    INTAGGW2VModel.save(s"${output}/models/INTAGGW2VModel")

    val INTAGGW2VModelB = spark.sparkContext.broadcast(INTAGGW2VModel)
    val INTAGGFn = applyModelFn(INTAGGW2VModelB, _)

    INTAGGW2VModel.getVectors.withColumn("predictions", udf(INTAGGFn).apply($"word"))
      .withColumn("model", lit("INTERACTIONS"))
      .withColumn("_prediction", explode($"predictions"))
      .withColumn("prediction", $"_prediction".getField("_1"))
      .withColumn("score", $"_prediction".getField("_2"))
      .select(cols.head, cols.tail:_*)
      .write.parquet(s"${output}/models/INTAGGPredictions")

    logger.info("generate model for FAERS aggregation and write")
    val AEAGG = AE.join(
      DR.filter($"efoId".isNotNull)
        .drop("geneId").distinct().join(D.drop("TAs"), Seq("efoId")), Seq("chemblId"))
      .groupBy($"meddraCode", $"ancestor", $"actionType", $"drugType")
      .agg(
        flatten(
          transform(
            sort_array(
              collect_set(struct($"score", $"efoId", $"chemblId"))
              , asc=false
            ),
            x => array(x.getField("efoId"), x.getField("chemblId"))
          )
        ).as("v")
      )

    val AEAGGW2V = new Word2Vec().setWindowSize(10).setNumPartitions(32).setMaxIter(5).setInputCol("v").setOutputCol("predictions")
    val AEAGGW2VModel = AEAGGW2V.fit(AEAGG)
    AEAGGW2VModel.save(s"${output}/models/AEAGGW2VModel")

    val AEAGGW2VModelB = spark.sparkContext.broadcast(AEAGGW2VModel)
    val AEAGGFn = applyModelFn(AEAGGW2VModelB, _)

    AEAGGW2VModel.getVectors.withColumn("predictions", udf(AEAGGFn).apply($"word"))
      .withColumn("model", lit("FAERS"))
      .withColumn("_prediction", explode($"predictions"))
      .withColumn("prediction", $"_prediction".getField("_1"))
      .withColumn("score", $"_prediction".getField("_2"))
      .select(cols.head, cols.tail:_*)
      .write.parquet(s"${output}/models/AEAGGPredictions")

    // val pr = predictions.groupBy($"word", $"prediction").agg(count($"model").as("n"), mean($"score").as("score"), collect_list($"model").as("m"))
    // val prtops = pr.join(D, !$"prediction".startsWith("CHEMBL") and $"efoId" === $"prediction", "left_outer").join(C, $"prediction".startsWith("CHEMBL") and $"chemblId" === $"prediction", "left_outer").withColumn("name", coalesce($"efoName", $"drugName")).drop("efoId", "chemblId", "efoName", "drugName").orderBy($"word", $"n".desc, $"score".desc)
  }
}

@main
def main(prefix: String, output: String): Unit =
  ETL(prefix, output)
