import $ivy.`com.typesafe:config:1.3.4`
import $ivy.`com.github.fommil.netlib:all:1.1.2`
import $ivy.`org.apache.spark::spark-core:2.4.3`
import $ivy.`org.apache.spark::spark-mllib:2.4.3`
import $ivy.`org.apache.spark::spark-sql:2.4.3`
import $ivy.`com.github.pathikrit::better-files:3.8.0`
import $ivy.`sh.almond::ammonite-spark:0.7.0`
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import better.files.Dsl._
import better.files._

object SchemaConverter {
  private def struct2SQL(struct: StructType): Seq[String] = {
    def fCast(sf: DataType): String = {
      // TODO esto tiene que ser programado mejor es un hack lo de nullable
      sf match {
        case _: BooleanType => "Nullable(UInt8)"
        case _: IntegerType => "Nullable(Int32)"
        case _: LongType => "Nullable(Int64)"
        case _: FloatType => "Nullable(Float32)"
        case _: DoubleType => "Nullable(Float64)"
        case _: StringType => "Nullable(String)"
        case s: StructType => s.fields.map(f => fCast(f.dataType)).mkString("Tuple(", ",", ")")
        case l: ArrayType => fCast(l.elementType).mkString("Array(", "", ")")
        case _ => "UnsupportedType"
      }
    }

    def metaCast(sf: StructField, data: String): String = {
      sf.dataType match {
        case a: ArrayType => a.elementType match {
          case _: ArrayType => s"$data default [[]]"
          case _ => s"$data default []"
        }

        case _ => data
      }
    }

    struct.fields.map(st => {
      "`" + st.name.replace("$", "__") + "` " + metaCast(st, fCast(st.dataType))
    })
  }

  def apply(schema: Option[StructType])(tableName: String): Option[String] = {
    schema.map(jo => {
      val tableTemplate =
        """
          |create table if not exists %s
          |%s
          |engine = Log;
        """.stripMargin

      tableTemplate.format(tableName, struct2SQL(jo).mkString("(\n", ",\n", ")"))
    })
  }
}

object Functions {
  def saveJSONSchemaTo(df: DataFrame, path: File, fileName: String = "schema"): Unit =
    (path / s"$fileName.json").createIfNotExists(createParents=true) < df.schema.json

  def saveSQLSchemaTo(df: DataFrame, path: File, tableName: String, fileName: String = "schema"): Unit =
    (path / s"$fileName.sql").createIfNotExists(createParents=true) < SchemaConverter(Some(df.schema))(tableName).get

  def loadSchemaFrom(filename: String): Option[StructType] = {
    val lines = filename.toFile.contentAsString
    Option(DataType.fromJson(lines).asInstanceOf[StructType])
  }
}

object NetworkDB {
  /** load string-db datasets using the mappings and the COG data
   * it needs the protein mapping to symbols gene ids
   */
  def buildStringDBTargetNetwork(protLinks: String, protInfo: String, targets: DataFrame)(implicit ss: SparkSession): DataFrame = {
    val prot2Name = ss
      .read
      .option("sep", "\t")
      .option("mode", "DROPMALFORMED")
      .csv(protInfo)
      .toDF("pid", "symbol", "protein_size", "annotation")
      .select("pid", "symbol")
      .filter(not(col("symbol").startsWith(lit("ENSG"))))
      .filter(not(col("symbol").startsWith(lit("HGNC:"))))
      .cache()

    val p2p = ss
      .read
      .option("sep", " ")
      .option("mode", "DROPMALFORMED")
      .csv(protLinks)
      .toDF("protein1", "protein2", "neighborhood", "fusion", "cooccurence", "coexpression",
        "experimental", "database", "textmining", "combined_score")
      .where(col("coexpression") > 0 and col("combined_score") > 700)

    val links = p2p.join(prot2Name,
      col("protein1") === col("pid"),
      "inner")
      .drop("pid", "protein1")
      .withColumnRenamed("symbol", "symbol_a")
      .join(prot2Name,
        col("protein2") === col("pid"),
        "inner")
      .withColumnRenamed("symbol", "symbol_b")
      .drop("protein2", "pid")
      .withColumnRenamed("symbol_b", "target_name")
      .join(targets.select("target_name", "target_id"), Seq("target_name"), "inner")
      .drop("target_name")
      .groupBy("symbol_a")
      .agg(collect_set(col("target_id")).as("_stringdb_set"))
      .selectExpr("symbol_a as target_name", "_stringdb_set")

    links.join(targets.select("target_name", "target_id"), Seq("target_name"), "inner")
      .withColumn("neighbours",array_union(array(col("target_id")), col("_stringdb_set")))
      .select("target_id", "neighbours")
  }

  def buildTargetNetwork(ndbPath: String, genesDF: DataFrame)(implicit ss: SparkSession): DataFrame = {
    val score = 0.45
    val p2pRaw = ss.read.json(ndbPath)
      .where(col("mi_score") > score or
        (array_contains(col("source_databases"), "intact") and
          (size(col("source_databases")) > 1)))
      .selectExpr(
        "interactorA_uniprot_name as A",
        "interactorB_uniprot_name as B")

    val p2p = p2pRaw.union(p2pRaw.toDF("B", "A").select("A", "B"))
      .distinct()

    val genes = genesDF
      .selectExpr("target_id as id",
        "uniprot_accessions as accessions")
      .withColumn("accession", explode(col("accessions")))
      .drop("accessions")
      .orderBy(col("accession"))
      .cache

    val p2pA = p2p.join(genes, genes("accession") === p2p("A"), "inner")
      .withColumnRenamed("id", "A_id")
      .drop("accession")

    val doubleJoint = p2pA.join(genes, genes("accession") === p2pA("B"), "inner")
      .withColumnRenamed("id", "B_id")
      .drop("accession")

    doubleJoint.groupBy(col("A_id").as("target_id"))
      .agg(collect_set(col("B_id")).as("neighbours"),
        approx_count_distinct(col("B_id")).as("degree"))
      .withColumn("neighbours", concat(array(col("target_id")), col("neighbours")))
      .select("target_id", "neighbours")
  }
  def buildDiseaseNetwork(diseasesDF: DataFrame)(implicit ss: SparkSession): DataFrame = {
    diseasesDF
      .withColumn("neighbours",col("descendants"))
      .selectExpr("disease_id", "neighbours")
  }
}

object Loaders {
  def loadTargets(path: String)(implicit ss: SparkSession): DataFrame = {
    val targetList = ss.read.json(path)
    val selectExpressions = Seq(
      "*",
      "id as target_id",
      "approved_symbol as target_name"
    )

    targetList.selectExpr(selectExpressions: _*)
  }

  def loadDiseases(path: String)(implicit ss: SparkSession): DataFrame = {
    val diseaseList = ss.read.json(path)

    val efos = diseaseList
      .withColumn("disease_id", substring_index(col("code"), "/", -1))
      .withColumn("ancestors", flatten(col("path_codes")))
      .drop("paths", "private", "_private", "path")

    val descendants = efos
      .where(size(col("ancestors")) > 0)
      .withColumn("ancestor", explode(col("ancestors")))
      // all diseases have an ancestor, at least itself
      .groupBy("ancestor")
      .agg(collect_set(col("disease_id")).as("descendants"))
      .withColumnRenamed("ancestor", "disease_id")

    efos.join(descendants, Seq("disease_id"))
      .selectExpr("disease_id", "ancestors", "descendants", "label as disease_name")
  }

  def loadEvidences(path: String)(implicit ss: SparkSession): DataFrame = {
    ss.read.json(path)
      .selectExpr(
        "target.id as target_id",
        "disease.id as disease_id",
        "to_json(evidence) as evs_obj",
        "to_json(unique_association_fields) As evs_unique_obj",
        "float(scores.association_score) as evs_score",
        "sourceID as evs_source",
        "transform(literature.references, x -> x.lit_id) as publications"
      )
      .where(col("evs_score").isNotNull and col("evs_score") > 0.0)
  }
}

object Transformers {
  def evidencesForAOTF(evs: DataFrame): DataFrame = {
    val evidences = evs
      .groupBy(col("target_id"), col("disease_id"), col("evs_source"))
      .agg(reverse(slice(array_sort(collect_list(col("evs_score"))),-1, 100)).as("scores"))
      .selectExpr("target_id", "disease_id", "evs_source", "scores")

    evidences.groupBy(col("target_id"), col("disease_id"))
      .pivot(col("evs_source"))
      .agg(first(col("scores")))
  }
}

@main
def main(drugFilename: String,
         targetFilename: String,
         diseaseFilename: String,
         interactionsFilename: String,
         evidenceFilename: String,
         stringdbInfoFilename: String,
         stringdbLinksFilename: String,
         outputPathPrefix: String): Unit = {
  val sparkConf = new SparkConf()
    .set("spark.driver.maxResultSize", "0")
    .setAppName("similarities-loaders")
    .setMaster("local[*]")

  implicit val ss = SparkSession.builder
    .config(sparkConf)
    .getOrCreate

  // AmmoniteSparkSession.sync()

  import ss.implicits._

  val targets = Loaders.loadTargets(targetFilename)
  val diseases = Loaders.loadDiseases(diseaseFilename)
  val targetsNetwork = NetworkDB.buildTargetNetwork(interactionsFilename, targets)
  val targetsStringDBNetwork =
    NetworkDB.buildStringDBTargetNetwork(stringdbLinksFilename, stringdbInfoFilename, targets)
  val diseasesNetwork = NetworkDB.buildDiseaseNetwork(diseases)
  val evidences = Loaders.loadEvidences(evidenceFilename)

  diseases
    .select("disease_id", "disease_name")
    .write.json(outputPathPrefix + "/disease_dict/")
  Functions.saveJSONSchemaTo(diseases, outputPathPrefix / "disease_dict")
  Functions.saveSQLSchemaTo(diseases, outputPathPrefix / "disease_dict" , "ot.disease_dict")

  targets
    .select("target_id", "target_name")
    .write.json(outputPathPrefix + "/target_dict/")
  Functions.saveJSONSchemaTo(targets, outputPathPrefix / "target_dict")
  Functions.saveSQLSchemaTo(targets, outputPathPrefix / "target_dict" , "ot.target_dict")

  targetsNetwork.write.json(outputPathPrefix + "/target_network_dict/")
  Functions.saveJSONSchemaTo(targetsNetwork, outputPathPrefix / "target_network_dict")
  Functions.saveSQLSchemaTo(targetsNetwork, outputPathPrefix / "target_network_dict" , "ot.target_network_dict")

  targetsStringDBNetwork.write.json(outputPathPrefix + "/target_network_stringdb_dict/")
  Functions.saveJSONSchemaTo(targetsStringDBNetwork, outputPathPrefix / "target_network_stringdb_dict")
  Functions.saveSQLSchemaTo(targetsStringDBNetwork, outputPathPrefix / "target_network_stringdb_dict" , "ot.target_network_stringdb_dict")

  diseasesNetwork.write.json(outputPathPrefix + "/disease_network_dict/")
  Functions.saveJSONSchemaTo(diseasesNetwork, outputPathPrefix / "disease_network_dict")
  Functions.saveSQLSchemaTo(diseasesNetwork, outputPathPrefix / "disease_network_dict" , "ot.disease_network_dict")

  evidences.write.json(outputPathPrefix + "/evidences/")
  Functions.saveJSONSchemaTo(evidences, outputPathPrefix / "evidences")
  Functions.saveSQLSchemaTo(evidences, outputPathPrefix / "evidences" , "ot.evidences")

  val aotf = Transformers.evidencesForAOTF(evidences)
  aotf.write.json(outputPathPrefix + "/evidences_aotf/")
  Functions.saveJSONSchemaTo(aotf, outputPathPrefix / "evidences_aotf")
  Functions.saveSQLSchemaTo(aotf, outputPathPrefix / "evidences_aotf" , "ot.evidences_aotf")

}
