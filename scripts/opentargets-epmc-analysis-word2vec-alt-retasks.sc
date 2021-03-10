import $file.resolvers
import $file.opentargetsFunctions
import $ivy.`ch.qos.logback:logback-classic:1.2.3`
import $ivy.`com.typesafe.scala-logging::scala-logging:3.9.2`
import $ivy.`com.typesafe:config:1.4.0`
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
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

import org.graphframes._

object SparkSessionWrapper extends LazyLogging {
  logger.info("Spark Session init")
  lazy val sparkConf = new SparkConf()
    .set("spark.driver.maxResultSize", "0")
    .set("spark.debug.maxToStringFields", "2000")
    .set("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
    .setAppName("etl-generation")
    .setMaster("local[*]")

  lazy val session: SparkSession =
    SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate
}

object ETL extends LazyLogging {
  def apply(modelFaers: String,
            modelPhenotypes: String,
            modelInteractions: String,
            modelEPMC: String,
            output: String) = {
    import SparkSessionWrapper._
    import session.implicits._

    val mFaers =
      session.sparkContext.broadcast(Word2VecModel.load(modelFaers).setOutputCol("faersSyns"))
    val mPhenotypes =
      session.sparkContext.broadcast(Word2VecModel.load(modelPhenotypes).setOutputCol("hpSyns"))
    val mInteractions =
      session.sparkContext.broadcast(Word2VecModel.load(modelInteractions).setOutputCol("intSyns"))
    val mEPMC =
      session.sparkContext.broadcast(Word2VecModel.load(modelEPMC).setOutputCol("epmcSyns"))

    val applyModelFn = (model: Broadcast[Word2VecModel], word: String) => {
      try {
        model.value.findSynonymsArray(word, 100).filter(_._2 > 0.1)
      } catch {
        case _ => Array.empty[(String, Double)]
      }
    }

    val mFaersFn = applyModelFn(mFaers, _)
    val mPhenotypesFn = applyModelFn(mPhenotypes, _)
    val mInteractionsFn = applyModelFn(mInteractions, _)

    val faersDF = mFaers.value.getVectors
      .withColumn("synonyms", udf(mFaersFn).apply($"word"))
      .withColumn("type", lit("drug"))

    val hpoDF = mPhenotypes.value.getVectors
      .withColumn("synonyms", udf(mPhenotypesFn).apply($"word"))
      .withColumn("type", lit("disease"))

    val intDF = mInteractions.value.getVectors
      .withColumn("synonyms", udf(mInteractionsFn).apply($"word"))
      .withColumn("type", lit("target"))

    val data = faersDF.unionByName(hpoDF).unionByName(intDF)
    // logger.info("fit the parametrised model and generate it to apply later to another DF")
    //    val mDF = session.read.parquet(matches).filter($"isMapped" === true)
    //    val matchesPerPMID = mDF
    //      .groupBy($"pmid")
    //      .agg(collect_list($"keywordId").as("terms"))
    //
    //    val matchesModel = makeWord2VecModel(matchesPerPMID,
    //      inputColName = "terms",
    //      outputColName = "synonyms"
    //    )
    //
    //    logger.info("saving the generated model for Word2Vec")
    //    matchesModel.save(output + "/matchesW2VModel")
    //
    //    logger.info("produce the list of unique terms (GP, DS, CD)")
    //    val keywords = mDF
    //      .select($"keywordId")
    //      .distinct()
    //
    //    val bcModel = session.sparkContext.broadcast(matchesModel)
    //    logger.info("compute the predictions to the associations DF with the precomputed model FPGrowth")
    //    val matchesWithSynonymsFn = udf((word: String) => {
    //      try {
    //        bcModel.value.findSynonymsArray(word, 50)
    //      } catch {
    //        case _ => Array.empty[(String, Double)]
    //      }
    //    })
    //
    //    val matchesWithSynonyms = keywords
    //      .withColumn("synonym", explode(matchesWithSynonymsFn($"keywordId")))
    //      .withColumn("synonymId", $"synonym".getField("_1"))
    //      .withColumn("synonymType", when($"synonymId" rlike "^ENSG.*", "GP")
    //        .when($"synonymId" rlike "^CHEMBL.*", "CD")
    //        .otherwise("DS"))
    //      .withColumn("keywordType", when($"keywordId" rlike "^ENSG.*", "GP")
    //        .when($"keywordId" rlike "^CHEMBL.*", "CD")
    //        .otherwise("DS"))
    //      .withColumn("synonymScore", $"synonym".getField("_2"))
    //      .drop("synonym")
    //
    //    logger.info("saving computed synonyms for each unique match")
    //    matchesWithSynonyms.write.parquet(output + "/matchesWithSynonyms")

  }
}

@main
  def main(modelFaers: String,
           modelPhenotypes: String,
           modelInteractions: String,
           modelEPMC: String,
           output: String): Unit =
    ETL(modelFaers, modelPhenotypes, modelInteractions, modelEPMC, output)
