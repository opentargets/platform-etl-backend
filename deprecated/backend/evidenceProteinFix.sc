import $file.common
import common._

import org.apache.spark.SparkConf
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.typesafe.config.Config
import org.apache.spark.storage.StorageLevel

/** It takes all evidences and fix the problem of having same protein accession
  * and multiple genes. The steps to fix it are
  * 1. build the inversed lut for each protein accession the corresponding genes
  * 2. if one evicence belonging to the multigene case is found then replace the
  *    protein id by the gene id
  *    and add to the unique fields the corresponding geneid in order to make unique
  *    the evidence
  * 3. write all back to jsonl gz but partitionby datasource
  * */
object EvidenceProteinFix extends LazyLogging {
  def buildLUT(genes: DataFrame, proteinColName: String, genesColName: String)(
      implicit ss: SparkSession
  ): DataFrame = {

    import ss.implicits._

    val UNI_ID_ORG_PREFIX = "http://identifiers.org/uniprot/"
    val ENS_ID_ORG_PREFIX = "http://identifiers.org/ensembl/"

    // build the accessions df
    val genesPerProtein = genes
      .selectExpr(
        s"concat('${ENS_ID_ORG_PREFIX}', id) as _id",
        s"transform(uniprot_accessions, x -> concat('${UNI_ID_ORG_PREFIX}', x)) as _accessions"
      )
      .withColumn(proteinColName, explode($"_accessions"))
      .groupBy(col(proteinColName))
      .agg(collect_set($"_id").as(genesColName))
      .where(col(genesColName).isNotNull and size(col(genesColName)) > 0)

    genesPerProtein
  }

  def apply(config: Config)(implicit ss: SparkSession) = {
    import ss.implicits._

    val evidenceProtSec = Configuration.loadEvidenceProteinFixSection(config)
    val common = Configuration.loadCommon(config)
    val dfs = Map(
      "targets" -> ss.read.json(common.inputs.target.path),
      "evidences" -> ss.read.json(evidenceProtSec.input)
    )

    val proteins =
      buildLUT(dfs("targets"), "accession", "ids")
        .orderBy($"accession".asc)

    val evsPre = dfs("evidences")
      .withColumn("dataset", col("sourceID"))
      .repartition(ss.sparkContext.defaultParallelism * 3)
      .persist(StorageLevel.DISK_ONLY)

    val evs = evsPre
      .join(broadcast(proteins), $"target.id" === $"accession", "left_outer")
      .withColumn("ids", when($"ids".isNull, array($"target.id")).otherwise($"ids"))
      .withColumn("target_id", explode($"ids"))
      .withColumn("ids_count", size($"ids"))
      .withColumn(
        "target_id",
        when($"ids_count" > 1, $"target_id")
          .otherwise(lit(null))
      )

    // get all columns from target except the id subfield
    val targetCols =
      evs.select("target.*").columns.withFilter(_ != "id").map(c => col("target." + c).as(c))

    val uafCols = evs
      .select("unique_association_fields.*")
      .columns
      .map(c => col("unique_association_fields." + c).as(c))

    // the new id subfield
    val idCol = $"target_id".as("id")
    val origId = $"target.id".as("id")

    val newUafCol = $"target_id".as("ensembl_id")

    val evss = evs
      .withColumn(
        "target",
        when($"ids_count" > 1, struct(targetCols :+ idCol: _*))
          .otherwise(struct(targetCols :+ origId: _*))
      )
      .withColumn("unique_association_fields", struct(uafCols :+ newUafCol: _*))
      .drop("target_id", "accesion", "ids", "ids_count")

    evss.write
      .partitionBy("dataset")
      .option("compression", "gzip")
      .json(s"${evidenceProtSec.output}/")

    evsPre.printSchema()
  }
}
