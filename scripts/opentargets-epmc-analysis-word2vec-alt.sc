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
import SparkSessionWrapper.session
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

import org.graphframes._

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
      .filter($"speciesB.taxon_id" === 9606)
    val evs = spark.read.parquet("21.02/outputs/evidences/succeeded")

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
      .join(D,Seq("efoId"), "left_outer")
      .drop("efoId")
      .withColumnRenamed("ancestor", "efoId")
      .selectExpr("chemblId", "geneId", "efoId", "drugType", "actionType", "TAs")

    //+----------+---------------+-----+
    //|geneA     |geneB          |count|
    //+----------+---------------+-----+
    //|A0A024A2C9|ENSG00000000971|1    |
    //|A0A024R5S0|ENSG00000105866|6    |
    //+----------+---------------+-----+
    val N = interactions.selectExpr("targetA as geneA", "targetB as geneB", "count")
      .groupBy($"geneA", $"geneB")
      .agg(sum($"count").as("count"))

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
      .withColumnRenamed("efoId", "directEfoId")
      .withColumnRenamed("ancestor", "efoId")
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
    val EDR = E.join(DR, Seq("efoId", "geneId")).withColumn("TA", explode($"TAs")).drop("TAs")

    //+-----------+------------+----------+--------------------+
    //|      efoId|         vId|actionType|                   v|
    //+-----------+------------+----------+--------------------+
    //|EFO_0000222|rs1060502568| INHIBITOR|[EFO_0000222, CHE...|
    //|EFO_0000228|RCV001161526|ANTAGONIST|[EFO_0000228, CHE...|
    //+-----------+------------+----------+--------------------+
    val EDRAGG = EDR.groupBy($"efoId", $"vId", $"actionType")
      .agg(flatten(transform(sort_array(collect_set(struct($"score", $"efoId", $"chemblId")), asc=false),
        c => array(c.getField("efoId"), c.getField("chemblId")) )).as("v"))

    logger.info("generate model for genetics aggregation and write")
    val EDRAGGW2V = new Word2Vec().setWindowSize(10).setNumPartitions(16).setMaxIter(10).setInputCol("v").setOutputCol("predictions")
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

    val DPAGG = DP.join(DR.drop("geneId"), Seq("efoId"))
      .distinct
      .groupBy($"phenotypeId", $"ancestor", $"drugType")
      .agg(flatten(collect_set(array($"efoId", $"chemblId"))).as("v"))

    logger.info("generate model for phenotypes aggregation and write")
    val DPAGGW2V = new Word2Vec().setWindowSize(10).setNumPartitions(16).setMaxIter(10).setInputCol("v").setOutputCol("predictions")
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

    //    logger.info("generate hp aggregation and write")
//    val hpsAgg = hpos.orderBy($"phenotypeId".asc, $"indirect".asc).groupBy($"phenotypeId").agg(collect_set($"indirect").as("diseaseIds")).filter(size($"diseaseIds") > 1)
//    hpsAgg.write.parquet(s"${output}/hposAggregation")
//
//    logger.info("compute xref from efo disease to meddra (it is not currently used)")
//    val med2efo = diseases.selectExpr("id", "dbXRefs").withColumn("xref", explode($"dbXRefs")).filter(col("xref").contains("DRA")).withColumn("meddraCode", element_at(split($"xref", ":"), 2)).selectExpr("id as diseaseId", "meddraCode")
//    val faersEFOLeft = faers.join(med2efo, Seq("meddraCode"), "left_outer").withColumnRenamed("chembl_id", "chemblId").selectExpr("meddraCode", "chemblId", "reaction_reactionmeddrapt as meddraName", "llr", "diseaseId")
//
//    logger.info("write faers pre aggregation with LUT for efo to meddra")
//    faersEFOLeft.write.parquet(s"${output}/faersPreAggregation")
//
//    val w = Window.partitionBy()
//    logger.info("join indications to compounds")
//    val drugs = compounds.join(indications, Seq("id"), "left_outer")
//      .withColumnRenamed("id", "chemblId")
//
//    logger.info("moas left join prefiously joint drugs and then explode targets to get one " +
//      "entry per target and again explode by indication so each indication per moa")
//    val drugTargets = moas.join(drugs, Seq("chemblId")).withColumn("targetId", explode($"targets")).drop("targets", "chemblIds").join(targets.selectExpr("id as targetId", "approvedSymbol"), Seq("targetId"))
//      .withColumn("indication", explode($"indications"))
//      .drop("indications", "count")
//      .drop("approvedIndications")
//
//    logger.info("write computed pre aggregation drugs")
//    drugTargets.write.parquet(s"${output}/drugsPreAggregation")
//
//    logger.info("aggregate interactions by targetA including itself into the list of interactions and write out")
//    val interactionsAgg = interactions.orderBy($"scoring".desc).groupBy($"targetA").agg(concat(array($"targetA"), collect_list($"targetB")).as("targetBs"))
//    interactionsAgg.write.parquet(s"${output}/interactionsAggregation")
//
//    logger.info("aggregate faers by meddra name the chemblids and write")
//    val faersAgg = faersEFOLeft.orderBy($"meddraName".asc, $"llr".desc)
//      .groupBy($"meddraName")
//      .agg(collect_list($"chemblId").as("chemblIds"))
//    faersAgg.write.parquet(s"${output}/faersAggregation")
//
//    logger.info("generate model for faers aggregation and write")
//    val w2vModel = new Word2Vec().setNumPartitions(16).setMaxIter(10).setInputCol("chemblIds").setOutputCol("predictions")
//    val model = w2vModel.fit(faersAgg)
//    model.save(s"${output}/models/faersW2VModel")
//
//    logger.info("generate model for interactions aggregation and write")
//    val interactW2VModel = new Word2Vec().setNumPartitions(16).setMaxIter(10).setInputCol("targetBs").setOutputCol("predictions")
//    val intModel = interactW2VModel.fit(interactionsAgg)
//    intModel.save(s"${output}/models/interactionsW2VModel")
//
//    logger.info("generate model for hp aggregation and write")
//    val hposW2VModel = new Word2Vec().setNumPartitions(16).setMaxIter(10).setInputCol("diseaseIds").setOutputCol("predictions")
//    val hposModel = hposW2VModel.fit(hpsAgg)
//    hposModel.save("/data/models/hpsW2VModel")
  }
}

@main
def main(prefix: String, output: String): Unit =
  ETL(prefix, output)
