import $file.common
import common._

import $ivy.`com.typesafe:config:1.3.4`
import $ivy.`com.typesafe.scala-logging::scala-logging:3.9.0`
import $ivy.`ch.qos.logback:logback-classic:1.2.3`
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

object TargetTransformers extends LazyLogging {

  implicit class ImplicitsTarget(val df: DataFrame) {

    def targetIndex: DataFrame = {
      val dfTarget = df.setIdAndSelectFromTargets
      dfTarget
    }

    def setIdAndSelectFromTargets: DataFrame = {
      val selectExpressions = Seq(
        "id",
        "approved_name as approvedName",
        "approved_symbol as approvedSymbol",
        "biotype as bioType",
        "case when (hgnc_id = '') then null else hgnc_id end as hgncId",
        "name_synonyms as nameSynonyms",
        "symbol_synonyms as symbolSynonyms",
        "struct(chromosome, gene_start as start, gene_end as end, strand) as genomicLocation"
      )

      val uniprotStructure =
        """
        |case
        |  when (uniprot_id = '' or uniprot_id = null) then null
        |  else struct(uniprot_id as id,
        |    uniprot_accessions as accessions,
        |    uniprot_function as functions)
        |end as proteinAnnotations
        |""".stripMargin

      df.selectExpr(selectExpressions :+ uniprotStructure: _*)
    }
  }
}

// This is option/step target in the config file
@main
def main(conf: String = "resources/amm.application.conf", outputPathPrefix: String = "output"): Unit = {
  import TargetTransformers.ImplicitsTarget

  val cfg = getConfig(conf)
  val listInputFiles = getInputFiles(cfg, "target")
  val inputDataFrame = SparkSessionWrapper.loader(listInputFiles)

  val dfTargetIndex = inputDataFrame("target").targetIndex

  SparkSessionWrapper.save(dfTargetIndex, outputPathPrefix + "/targets")
  //dfTargetIndex.write.json(outputPathPrefix + "/targets")
}
