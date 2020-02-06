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
import org.apache.spark.storage.StorageLevel

/** It takes all evidences and fix the problem of having gwas studies in both
 * datasources GWAS and Genetics Platform. Therefore we need to remove from gwas
 * the studies we are currently adding from Genetics Platform
 * */
object EvidenceGWASFix extends LazyLogging {
  def apply(config: Config)(implicit ss: SparkSession) = {
    import ss.implicits._

    val evidenceGWASSec = Configuration.loadEvidenceGWASFixSection(config)
    val dfs = Map(
      "studies" -> ss.read.parquet(evidenceGWASSec.studies),
      "evidences" -> ss.read.json(evidenceGWASSec.input)
    )

    val studies = broadcast(
      dfs("studies")
        .select($"study_id")
        .filter($"study_id".startsWith("GCST"))
        .withColumn("study_id", substring_index($"study_id", "_", 1))
        .orderBy("study_id")
    )

    val evsPre = dfs("evidences")
      .join(studies,
        dfs("evidences")("unique_association_fields.study_name") === studies("study_id"),
      "left_anti")

    evsPre.write
      .partitionBy("dataset")
      .option("compression", "gzip")
      .json(s"${evidenceGWASSec.output}/")
  }
}
