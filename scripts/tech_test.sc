import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val ss: SparkSession = ???
import ss.implicits._
//val path = "/home/jarrod/development/ot_technical_test/"
val path = "/home/jarrod/recruitment/"
val targetDf = ss.read.parquet(s"$path/targets").select('id as "tid", 'approvedSymbol)
val diseaseDf = ss.read.parquet(s"$path/diseases").select('id as "did", 'name)
val evaRawDf = ss.read.parquet(s"$path/eva")

// used to see all scores
//val scores = evaRawDf
//  .select('diseaseId, 'targetId, 'score)
//  .groupBy('diseaseId, 'targetId)
//  .agg(sort_array(collect_list('score), asc = false) as "scores")
//  .orderBy('targetId).show(false)

val evaDf = evaRawDf
  .select('diseaseId, 'targetId, 'score)
  .groupBy('diseaseId, 'targetId)
  .agg(sort_array(collect_list('score), asc = false) as "scores",
       percentile_approx('score, lit(0.5), lit(10000)) as "median")
  .select('diseaseId, 'targetId, 'median, slice('scores, 1, 3) as "top3")

val evaAnnotated = evaDf
  .join(targetDf, 'tid === 'targetId)
  .join(diseaseDf, 'did === 'diseaseId)
  .drop("tid", "did")
  .orderBy('median)

evaAnnotated.write.json(s"$path/output")

val evaByTarget = evaRawDf
  .groupBy('targetId)
  .agg(collect_list('diseaseId) as "diseases")
  .filter(size('diseases) >= 2)

val e = evaByTarget.crossJoin(
  evaByTarget
    .withColumnRenamed("diseases", "d")
    .withColumnRenamed("targetId", "t")
).select('targetId, 't, size(array_intersect('d, 'diseases)) as "s", 'diseases, 'd)
  .filter(col("s") >= 2
    && col("targetId") != col("t")) // remove cross join duplication to self
  .cache

e.count

evaByTarget.count

