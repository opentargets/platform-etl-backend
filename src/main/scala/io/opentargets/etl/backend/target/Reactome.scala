package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.{col, collect_set, element_at, struct}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class Reactome(pathwayId: String, pathway: String, topLevelTerm: String)

case class Reactomes(id: String, pathways: Seq[Reactome])

object Reactome extends LazyLogging {

  /** @param reactomePathways raw reactome file available from [[https://reactome.org/download-data]].
    * @param reactomeEtlDF    as computed by ETL
    * @param sparkSession
    * @return dataframe of uniprotIds with related pathways
    */
  def apply(reactomePathways: DataFrame, reactomeEtlDf: DataFrame)(implicit
      ss: SparkSession
  ): Dataset[Reactomes] = {
    import ss.implicits._

    logger.info("Computing reactome pathways for target.")

    val rpColumns = Seq(
      "ensemblId",
      "reactomeId",
      "url",
      "eventName",
      "eventCode",
      "species"
    )
    // dataframe with ensembl Id and reactome pathway
    val rpDf = reactomePathways
      .toDF(rpColumns: _*)
      .filter(col("species") === "Homo sapiens")
      .filter(col("ensemblId").startsWith("ENSG"))
      .select("ensemblId", "reactomeId", "eventName")

    // dataframe with top level term
    val topLevelTerms = reactomeEtlDf
      .select(col("id"), element_at(element_at(col("path"), 1), 1) as "tlId")
      .join(reactomeEtlDf.select(col("id") as "tlId", col("label") as "topLevelTerm"), Seq("tlId"))
      .select(col("id") as "reactomeId", col("topLevelTerm"))

    // add top level terms to reactome pathways
    val reactomePathwaysDf = rpDf.join(topLevelTerms, Seq("reactomeId"))

    val reactomeGroupedByEnsembl = reactomePathwaysDf
      .groupBy("ensemblId")
      .agg(
        collect_set(
          struct(
            col("reactomeId") as "pathwayId",
            col("eventName") as "pathway",
            col("topLevelTerm")
          )
        ) as "pathways"
      )
      .withColumnRenamed("ensemblId", "id")
      .as[Reactomes]

    reactomeGroupedByEnsembl

  }
}
