import scala.sys.process._

def cleanAll(): Unit = {
  val path = "gs://open-targets-pre-data-releases/22.11/output/etl/json/"
  val dirs = Seq(
    "literatureIndex",
//    "matches",
    "W2VModel",
//    "cooccurrences",
//    "failedMatches",
    "trainingSet",
    "vectors"
  )
  dirs.foreach { d =>
    println(s"Deleting $d")
    s"gsutil -m rm -r $path$d".!
  }
}
import java.io.File
import scala.io.Source
val badFiles = new File("/mnt/disks/literature/bad_files.txt")
def deleteFiles(path: File): Unit = {
  val files = Source.fromFile(path).getLines().toList
  for {
    file <- files
  } yield {
    val dir = "/mnt/disks/literature" + file
    println(s"Deleting $dir")
    s"rm $dir".!
  }
}
deleteFiles(badFiles)
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
val ss: SparkSession = ???
import ss.implicits._
val out = "/mnt/disks/literature/ft_bad"
val df = ss.read
  .json("/mnt/disks/literature/Full-text/23_03_2022/*.jsonl")
  .withColumn("file", input_file_name)
  .persist(StorageLevel.MEMORY_AND_DISK)
val df2 = df.filter('_corrupt_record.isNotNull).select('file).distinct
df2.coalesce(1).write.json(out)

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column

def writeCSV(df:DataFrame, path: String): Unit = df.coalesce(1).write.option("header", true).csv(path)

val abstractDf = ss.read.parquet("/mnt/disks/literature/lit_parquet/literature_parquet/Abstracts")
val ftDf = ss.read.parquet("/mnt/disks/literature/lit_parquet/literature_parquet/Full-text")

def senCount(df: DataFrame): DataFrame = df
  .select('pmcid, 'pmid, 'pprid, size('sentences) as "sen_count")
  .filter('pmcid.isNotNull or 'pmid.isNotNull or 'pprid.isNotNull)
  .orderBy(desc("sen_count"))

val absSenCount = abstractDf.transform(senCount)
val ftSenCount = ftDf.transform(senCount)

writeCSV(absSenCount, "abs_sentence_count")
writeCSV(ftSenCount, "fts_sentence_count")

val rec1 = ftDf.select('pmcid, 'pubdate, 'timestamp).filter('pmcid === "PMC4405523")

/**
  * The valuable information is the sentences, we would assume that any information in the sentences which is the same
  * can be discarded as we retain the same amount of information, but get rid of the excess data.
  */
def countByHash(df: DataFrame): Long = df
  .select('pmcid, 'pmid, 'pprid, 'pubDate, hash('sentences) as "hash", 'timestamp)
  .orderBy("hash")
  .dropDuplicates("hash")
  .count

val ftDedupCount = countByHash(ftDf)
val absDedupCount = countByHash(abstractDf)

/*
ftDedupCount: Long = 1374503L
absDedupCount: Long = 18629961L
 */


val outPath = "/mnt/disks/literature/ft_dedup"
val ftClean = ftDf
  .withColumn("hash", hash('sentences))
  .dropDuplicates("hash")
  .orderBy('pmcid, 'pmid, 'timestamp)
  .select(
    'organisms,
    'pmcid,
    'pmid,
    'pubDate,
    'sentences,
    'timestamp,
  )

// === count of publications by date
def countByDate(df:DataFrame): DataFrame = df
  .select('pmcid, 'pmid, to_date('timestamp) as "date")
  .groupBy('date)
  .agg(countDistinct('pmcid) as "pmcid", countDistinct('pmid) as "pmid")
  .cache
def orderBy(df: DataFrame, col: Column): DataFrame = df.orderBy(col.desc)

def sortByDate(df:DataFrame):DataFrame = orderBy(countByDate(df), 'date)
def sortByPmidCount(df:DataFrame):DataFrame = orderBy(countByDate(df), 'pmid)


// Abstracts
writeCSV(sortByDate(abstractDf), "abstract_by_date")
writeCSV(sortByDate(ftDf), "ft_by_date")
writeCSV(sortByPmidCount(abstractDf), "abstract_by_count")
writeCSV(sortByPmidCount(ftDf), "ft_by_count")
// === count of publications by pmid

// === bucket values
// https://spark.apache.org/docs/2.2.0/ml-features.html#bucketizer
//import $ivy.`org.apache.spark::spark-mllib:3.2.1`
import org.apache.spark.ml.feature.Bucketizer
val splits = Array(Double.NegativeInfinity, 0, 10, 100, 1000, 10000)
val bucketizer = new Bucketizer().setInputCol("sen_count").setOutputCol("bucketedFeatures").setSplits(splits)
val bucketedFt = bucketizer.transform(ftSenCount.select('sen_count))
bucketedFt.show()
val splits = Array(Double.NegativeInfinity, 0, 10, 100, 1000, 10000, Double.PositiveInfinity)
val bucketizer = new Bucketizer().setInputCol("sen_count").setOutputCol("bucketedFeatures").setSplits(splits)
val bucketedFt = bucketizer.transform(ftSenCount.select('sen_count))
bucketedFt.show()
bucketedFt.groupBy("bucketedFeatures").count

//===
val f = new File("/home/jarrod/driver/conf/literature_jobs")
val jobs = Source.fromFile(f).getLines.toList
val logs = jobs.map(j => s"gcloud dataproc jobs describe $j".!!)
