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

object EvidenceProteinFix extends LazyLogging {
  def buildLUT(genes: DataFrame, proteinColName: String, genesColName: String)(
      implicit ss: SparkSession): DataFrame = {

    import ss.implicits._

    val UNI_ID_ORG_PREFIX = "http://identifiers.org/uniprot/"
    val ENS_ID_ORG_PREFIX = "http://identifiers.org/ensembl/"

    // build the accessions df
    val genesPerProtein = genes
      .selectExpr(
        s"concat('${ENS_ID_ORG_PREFIX}', id) as _id",
        s"transform(uniprot_accessions, x -> concat('${UNI_ID_ORG_PREFIX}', x)) as _accessions")
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
      "targets" -> ss.read.json(common.inputs.target),
      "evidences" -> ss.read.json(evidenceProtSec.input)
    )

    // // get all columns except foo.baz
    //val structCols = df.select($"foo.*")
    //    .columns
    //    .filter(_!="baz")
    //    .map(name => col("foo."+name))
    //
    //df.withColumn(
    //    "foo",
    //    struct((structCols:+myUDF($"foo.baz").as("baz")):_*)
    //)

    val proteins =
      buildLUT(dfs("targets"), "accession", "ids")
        .orderBy($"accession".asc)

    val evsPre = dfs("evidences")
      .withColumn("dataset", substring_index(substring_index(input_file_name(), ".", 1), "/", -1))
      .repartition(ss.sparkContext.defaultParallelism * 3)
      .persist(StorageLevel.DISK_ONLY)

    val evs = evsPre
      .join(broadcast(proteins), $"target.id" === $"accession", "left_outer")
      .withColumn("ids", when($"ids".isNull, array($"target.id")).otherwise($"ids"))
      .withColumn("target_id", explode($"ids"))
      .withColumn("ids_count", size($"ids"))
      .withColumn("target_id",
                  when($"ids_count" > 1, $"target_id")
                    .otherwise(lit(null)))

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
      .withColumn("target",
                  when($"ids_count" > 1, struct(targetCols :+ idCol: _*))
                    .otherwise(struct(targetCols :+ origId: _*)))
      .withColumn("unique_association_fields", struct(uafCols :+ newUafCol: _*))
      .drop("target_id", "accesion", "ids", "ids_count")

    evss.write
      .partitionBy("dataset")
      .option("compression", "gzip")
      .json(s"${evidenceProtSec.output}/")

    evsPre.printSchema()
  }
}
