package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{array, col, typedLit}

/** @param id   ensembl gene id eg. ENSGXXXX
  * @param xRef database cross reference
  */
case class GeneWithDbXRef(id: String, xRef: Seq[IdAndSource])

/** ProjectScores from [[https://score.depmap.sanger.ac.uk/ Cancer Depdendency Map]] */
object ProjectScores extends LazyLogging {
  def apply(projectScore: DataFrame, dependencyMatrix: DataFrame)(implicit
      sparkSession: SparkSession
  ): Dataset[GeneWithDbXRef] = {
    import sparkSession.implicits._
    logger.info("Calculating project scores.")

    val projectScoreIdsDF = projectScore
      .filter(col("ensembl_gene_id").isNotNull)
      .select(
        col("gene_id").as("id"),
        col("ensembl_gene_id"),
        col("hgnc_symbol")
      )

    val geneWithDependencyScoreDF = {
      dependencyMatrix.columns
        .withFilter(_ != "Gene")
        .map(wrapColumnNamesWithPeriodCharacters)
        .foldLeft(dependencyMatrix.withColumn("total", typedLit(0)))((df, c) => {
          df.withColumn("total", col("total") + col(c))
        })
        .select("Gene", "total")
        .filter(col("total") > 0)
    }

    val projectScoreDS = geneWithDependencyScoreDF
      .join(projectScoreIdsDF, col("Gene") === projectScoreIdsDF("hgnc_symbol"))
      .select("ensembl_gene_id", "id")
      .withColumn("source", typedLit("ProjectScore"))
      .transform(nest(_, List("id", "source"), "xRef"))
      .withColumn("xRef", array(col("xRef")))
      .withColumnRenamed("ensembl_gene_id", "id")
      .as[GeneWithDbXRef]

    projectScoreDS
  }
}
