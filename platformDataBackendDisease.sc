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
import org.apache.spark.sql.functions.col


object Loaders {
  /** Load Diseases file */
  def loadDiseases(path: String)(implicit ss: SparkSession): DataFrame = {
    val diseaseList = ss.read.json(path)
    diseaseList
  }

  /** Load Evidences file */
  def loadEvidences(path: String)(implicit ss: SparkSession): DataFrame = {
    val evidences = ss.read.json(path)
    evidences
  }
}

object Transformers {
  implicit class Implicits (val df: DataFrame) {

    def setIdAndSelectFromDiseases(ss: SparkSession): DataFrame = {
      val genAncestors = udf((codes: Seq[Seq[String]]) =>
        codes.view.flatten.toSet.toSeq)

      val efos_summary = df
        .withColumn("id", substring_index(col("code"), "/", -1))
        .withColumn("ancestors", genAncestors(col("path_codes")))
        .withColumn(
          "isTherapeuticArea", size(genAncestors(col("path_codes"))) === 1).as("isTherapeuticArea")
        .withColumn("leaf", size(col("children")) === 0)
        .withColumn("sources",struct(
          (col("code")).as("url"),
          col("id").as("name")
        ))
        .withColumn("phenotypesCount", size(col("phenotypes")))
        .withColumnRenamed("phenotypes", "sourcePhenotypes")
        .drop("paths", "private", "_private", "path")
        .withColumn("phenotypes",struct(
          col("phenotypesCount"),col("sourcePhenotypes").as("sources")
        ))
        .withColumn(
          "ontology", struct(
            (col("isTherapeuticArea")).as("isTherapeuticArea"),
            col("leaf").as("leaf"),
            col("sources").as("sources")
          )
        )

      val descendants = efos_summary
        .where(size(col("ancestors")) > 0)
        .withColumn("ancestor", explode(col("ancestors")))
        // all diseases have an ancestor, at least itself
        .groupBy("ancestor")
        .agg(collect_set(col("id")).as("descendants"))
        .withColumnRenamed("ancestor", "id")

      val efos= efos_summary.join(descendants, Seq("id"))
        .withColumn(
          "is_id_included_descendent", array_contains(col("descendants"), col("id"))
        )

      //val included = efos.filter(col("is_id_included_descendent") === false).select("id","is_id_included_descendent").show(10, false)

      // Rename the columns following the graphql schema.
      val lookup = ss.sparkContext.broadcast(Map("label" -> "name", "definition" -> "description", "efo_synonyms" -> "synonyms", "therapeutic_codes" -> "therapeuticAreas" ))
      efos.select(efos.columns.map(colname => col(colname).as(lookup.value.getOrElse(colname, colname))): _*)
    }

    def get_DiseaseDetailDrugs(ss: SparkSession, all_diseases: DataFrame): DataFrame = {

      val df_drugs_extended = df
        .where(col("private.datatype") === "known_drug")
        .withColumn("disease_id", col("disease.id"))
        .withColumn("drug_id", substring_index(col("drug.id"), "/", -1))
        .join(all_diseases, expr("array_contains(descendants,disease_id)"), "inner")

      val df_details = df_drugs_extended.groupBy(col("id"),
        col("disease"),col("target"),col("drug"),
        col("ontology"), col("phenotypes"), col("drug_id"),
        col("name"),
        col("description"),
        col("therapeuticAreas"),
        col("synonyms"))
        .count()
        .withColumn("target_details",
          struct( col("target.id").as("id"),col("target.gene_info.symbol").as("symbol"),
            col("target.gene_info.name").as("name")))
        .withColumn("disease_details", struct(
          col("disease.id").as("id"),col("disease.efo_info.label").as("name")
        ))
        .withColumn("drug_details", struct(
          col("drug.id").as("id"), col("drug.molecule_name").as("name"),
          col("drug.molecule_type").as("type")
        ))
        .drop("drug","target","disease")
        .withColumn("EvidenceRowDrugs_single_row", struct(
          col("drug_details").as("drug"), col("target_details").as("target"),
          col("disease_details").as("disease")
        ))
        .drop("drug_details","target_details","disease_details")

      val df_evidences_rows = df_details.groupBy(col("id"),col("ontology"), col("phenotypes"),
        col("name"),
        col("description"),
        col("therapeuticAreas"),
        col("synonyms"))
        .agg(collect_set(col("drug_id")).as("associated_drugs"),
          collect_list(col("EvidenceRowDrugs_single_row")).as("drugs_rows"))
        .withColumn("drugsCount", size(col("associated_drugs")))
        .withColumn("drugsSources", expr("transform(associated_drugs, drug -> struct(drug as name, concat_ws('/', '',drug) as url ))") )
        .withColumn("drugs",struct(
          (col("drugsCount")),col("drugsSources").as("sources"))
        ).drop("drugsCount","drugsSources")
        .withColumn("summaries",struct(
          col("ontology"),
          col("phenotypes"),
          col("drugs")))
        .drop("ontology","phenotypes","drugs","associated_drugs","drugsSources","drugsCount")

      println(df_evidences_rows.filter(col("id")==="EFO_0000311").show())
      df_evidences_rows

    }

  }
}

@main
def main(diseaseFilename: String,
         evidenceFilename: String, outputPathPrefix: String): Unit = {
  val sparkConf = new SparkConf()
    .set("spark.debug.maxToStringFields", "2000")
    .setAppName("diseases-aggregation")
    .setMaster("local[*]")

  implicit val ss = SparkSession.builder
    .config(sparkConf)
    .getOrCreate

  // AmmoniteSparkSession.sync()

  import ss.implicits._
  import Transformers.Implicits

  val diseases = Loaders.loadDiseases(diseaseFilename)
  val evidences = Loaders.loadEvidences(evidenceFilename)

  val all_diseases = diseases
    .setIdAndSelectFromDiseases(ss)

  //all_diseases.printSchema()
  println("Diseases"+all_diseases.count())
  println("Evidences"+ evidences.count())

  val fds_base = evidences.drop("evidence","id","access_level","literature")

  val fds_summaries_and_details = fds_base.get_DiseaseDetailDrugs(ss, all_diseases)

  fds_summaries_and_details.write.json(outputPathPrefix + "/diseases")

}
