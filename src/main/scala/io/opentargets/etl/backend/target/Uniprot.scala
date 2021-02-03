package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Uniprot extends LazyLogging {

  val id = "uniprotId"

  def apply(dfRaw: DataFrame)(implicit ss: SparkSession): DataFrame = {
    logger.info("Processing Uniprot inputs")
    lazy val uniprotDfRaw =
      ss.read.format("com.databricks.spark.xml").option("rowTag", "entry").load("*.xml")

    /*
    res51: Array[String] = Array(
        "_created",
        "_dataset",
        "_modified",
        "_version",
        "accession",
        "comment",
        "dbReference",
        "evidence",
        "feature",
        "gene",
        "geneLocation",
        "keyword",
        "name",
        "organism",
        "protein",
        "proteinExistence",
        "reference",
        "sequence"
      )
     */
    val uniprotDfWithId = uniprotDfRaw
      .select(
        col("accession").as("uniprotAccessions"),
        col("comment"),
        col("gene")
      )
      .filter(size(col("uniprotAccessions")) > 0) // null return -1 so remove those too
      .withColumn(id, expr("uniprotAccessions[0]"))

    val commentsDf = processComments(uniprotDfWithId)

    ???
  }

  /**
    *
    * @param df uniprot with columns uniprotId and comments
    * @return dataset with fields from comments structure
    */
  def processComments(df: DataFrame)(implicit ss: SparkSession): Dataset[UniprotComment] = {
    import ss.implicits._
    // Simple comments have a key in _type and the value in text._VALUE
    def simpleComment(key: String,
                      comments: DataFrame,
                      outputName: Option[String] = None): DataFrame =
      comments
        .filter(s"_type == '$key'")
        .select(id, "_type", "text._VALUE")
        .drop("_type")
        .groupBy(id)
        .agg(collect_set("_VALUE").as(outputName.getOrElse(key)))

    val comments = df
      .select(col(id), explode(col("comment")))
      .select(id, "col.*")
    val functions = simpleComment("function", comments, Some("functions"))
    val pathways = simpleComment("pathway", comments, Some("pathways"))
    val subunit = simpleComment("subunit", comments, Some("subunits"))
    val similarity = simpleComment("similarity", comments, Some("similarities"))
    val subcellularLocation = comments
      .select(col(id), explode(col("subcellularLocation")))
      .select(col(id), explode(col("col.location._VALUE")).as("subcellularLocation"))
      .groupBy(id)
      .agg(collect_set("subcellularLocation").as("subcellularLocation"))

    val dfs: Seq[DataFrame] = Seq(functions, pathways, subunit, similarity, subcellularLocation)
    val combinedComments: DataFrame = dfs
      .reduce((a, b) => a.join(b, Seq(id), "outer"))

    combinedComments.as[UniprotComment]
  }

  /**
    *
    * @param df
    * @return dataframe of [uniprotId, name, synonym]
    */
  def processGene(df: DataFrame): DataFrame = {
    val nameAndSynonyms = df
      .select(col(id), explode(col("gene.name")).as("gene"))
      .select(col(id), explode(col("gene")).as("gene"))
      .select(col(id), col("gene._VALUE").as("name"), col("gene._type").as("nameType"))

    val approvedSymbol = nameAndSynonyms.where("nameType = 'primary'").select(id, "name")
    val symbolSynonyms = nameAndSynonyms
      .where("nameType = 'synonym'")
      .groupBy(id)
      .agg(collect_set(col("name")).as("synonyms"))

    approvedSymbol.join(symbolSynonyms, Seq(id), "outer")
  }

  case class UniprotComment(uniprotId: String,
                            functions: Array[String],
                            pathways: Array[String],
                            subunits: Array[String],
                            similarities: Array[String],
                            subcellularLocation: Array[String])

}
