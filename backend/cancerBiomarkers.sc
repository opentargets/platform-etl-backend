import $file.common
import common._

import $ivy.`ch.qos.logback:logback-classic:1.2.3`
import $ivy.`com.typesafe.scala-logging::scala-logging:3.9.2`
import $ivy.`com.typesafe:config:1.4.0`
import $ivy.`com.github.fommil.netlib:all:1.1.2`
import $ivy.`org.apache.spark::spark-core:2.4.3`
import $ivy.`org.apache.spark::spark-mllib:2.4.3`
import $ivy.`org.apache.spark::spark-sql:2.4.3`
import $ivy.`com.github.pathikrit::better-files:3.8.0`
import $ivy.`sh.almond::ammonite-spark:0.7.0`
import org.apache.spark.SparkConf
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.typesafe.config.Config

object CancerBiomarkersHelpers {
  implicit class AggregationHelpers(df: DataFrame)(implicit ss: SparkSession) {
    import Configuration._
    import ss.implicits._

    def setIdAndCancerBiomarkers: DataFrame = {
      val selectExpressions = Seq(
        "id",
        "cancerbiomarkers as cancerBiomarkers"
      )

      val dfBiomarkers = df
        .selectExpr(selectExpressions: _*)
        .withColumn(
          "hasCancerBiomarkers",
          when(size(col("cancerBiomarkers")) > 0, true).otherwise(false)
        )
        .withColumn("cancerBiomarkerEntries", explode_outer(col("cancerBiomarkers.biomarker")))
        .distinct
        .groupBy("id", "hasCancerBiomarkers", "cancerBiomarkers")
        .agg(
          expr(
            "case when cancerBiomarkers is null then 0 else count(*) end as cancerBiomarkerCount"
          )
        )
        .withColumn(
          "cancerBiomarkersDetails",
          when(
            size(col("cancerBiomarkers")) > 0,
            expr(
              "transform(cancerBiomarkers, cb -> named_struct('biomarker', cb.biomarker,'drugName',  cb.drugfullname,'associationType',cb.association, 'evidenceLevel', cb.evidencelevel,'diseases', cb.diseases.id, 'sources_other', cb.references.other, 'sources_pubmed', cb.references.pubmed))"
            )
          ).otherwise(null)
        )
        .drop("cancerBiomarkers")
        .withColumn(
          "diseaseCount",
          when(
            col("hasCancerBiomarkers") === true,
            size(array_distinct(flatten(col("cancerBiomarkersDetails.diseases"))))
          ).otherwise(0)
        )
        .withColumn(
          "drugCount",
          when(
            col("hasCancerBiomarkers") === true,
            size(array_distinct(col("cancerBiomarkersDetails.drugName")))
          ).otherwise(0)
        )

      dfBiomarkers
    }
  }
}

// This is option/step cancerbiomarkers in the config file
// GENE ID with a subset of cases.
object CancerBiomarkers extends LazyLogging {
  def apply(config: Config)(implicit ss: SparkSession) = {
    import ss.implicits._
    import CancerBiomarkersHelpers._

    val common = Configuration.loadCommon(config)
    val mappedInputs = Map("target" -> common.inputs.target)
    val inputDataFrame = SparkSessionWrapper.loader(mappedInputs)

    val cancerBioDF = inputDataFrame("target").setIdAndCancerBiomarkers

    SparkSessionWrapper.save(cancerBioDF, common.output + "/cancerBiomarkers")

  }
}
