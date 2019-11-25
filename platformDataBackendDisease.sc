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
import org.apache.spark.sql._
import org.apache.spark.sql.types._

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

  /** Load Relation file */
  def loadDDR(path: String)(implicit ss: SparkSession): DataFrame = {
    val ddr = ss.read.json(path)
    ddr
  }



}

object Transformers {
  implicit class Implicits (val df: DataFrame) {

    def joinAll(ss: SparkSession, dfDDR: DataFrame, dfEvidence: DataFrame): DataFrame = {

      val dfKnowDrugs = dfEvidence
        .where(col("`type`") === "known_drug")
        .withColumn("disease_id", col("disease.id"))
        .withColumn("drug_id", substring_index(col("drug.id"), "/", -1))


      val dfRelatedDiseases = df.join(dfDDR, dfDDR("subject.id") === df("id"), "left")
        .withColumn("relatedDiseasesSingleRow",  when(col("object.id").isNotNull,
          struct(col("object.id").as("B"),
            col("scores.overlap").as("score"),
            col("subject.links.targets_count").as("targetCountA"),
            col("object.links.targets_count").as("targetCountB"),
            col("counts.shared_count").as("targetCountAAndB"),
            col("counts.union_count").as("targetCountAOrB"))))

      val dfGroupRelatedAndDrug = dfRelatedDiseases.groupBy(col("id"),
        col("name"), col("description"), col("synonyms"), col("phenotypes"),
        col("therapeuticAreas"), col("parentIds"), col("isTherapeuticArea"), col("children"),
        col("leaf"), col("ontology"), col("descendants"))
        .agg(collect_list(col("relatedDiseasesSingleRow")).as("detailsRelatedDiseasesRows"))
        .withColumn("relatedDiseasesCount", size(col("detailsRelatedDiseasesRows")))
        .withColumn("relatedDiseases",
          struct(col("relatedDiseasesCount").as("count"),
            col("detailsRelatedDiseasesRows").as("rows")
          ))
        .drop("relatedDiseasesCount", "detailsRelatedDiseasesRows")
        .join(dfKnowDrugs, expr("array_contains(descendants,disease_id)"), "left")


      val dfDiseases = dfGroupRelatedAndDrug.groupBy(col("id"),
        col("disease"),col("target"),col("drug_id"),
        col("ontology"), col("phenotypes"),
        col("name"), col("description"), col("therapeuticAreas"),
        col("parentIds"),col("children"),col("synonyms"), col("relatedDiseases"))
        .count()
        .withColumn("EvidenceRowDrugs_single_row", struct(
          col("drug_id").as("drug"), col("target.id").as("target")
        ))
        .drop("disease","target")
        .groupBy(col("id"),col("ontology"), col("phenotypes"),
          col("name"), col("description"), col("therapeuticAreas"),
          col("parentIds"),col("children"),col("synonyms"),
          col("relatedDiseases"))
        .agg(collect_set(col("drug_id")).as("associated_drugs"),
          collect_list(col("EvidenceRowDrugs_single_row")).as("drugs_rows"))
        .withColumn("drugs",struct(
          size(col("associated_drugs")).as("count"),
          col("drugs_rows").as("rows"))
        ).drop("associated_drugs","drugs_rows")

      dfDiseases

    }

    def setIdAndSelectFromDiseases(ss: SparkSession): DataFrame = {
      val dfPhenotypeId =  df
        .withColumn("sourcePhenotypes",
          when(size(col("phenotypes")) > 0, expr("transform(phenotypes, phRow -> named_struct('url', phRow.uri,'name',  phRow.label, 'id', substring_index(phRow.uri, '/', -1)))")))

      val efosSummary = dfPhenotypeId
        .withColumn("id", substring_index(col("code"), "/", -1))
        .withColumn("ancestors", flatten(col("path_codes")))
        .withColumn("parents",
          array_except(array_distinct(flatten(col("path_codes"))),
            array(col("id"))) )
        .withColumn("phenotypesCount", size(col("phenotypes")))
        .drop("paths", "private", "_private", "path")
        .withColumn("phenotypes",struct(
          col("phenotypesCount").as("count"),
          col("sourcePhenotypes").as("rows")
        ))
        //.withColumn("test",when(size(col("sourcePhenotypes")) > 0, extractIdPhenothypes(col("sourcePhenotypes"))).otherwise(col("sourcePhenotypes")))
        .withColumn(
          "isTherapeuticArea", size(flatten(col("path_codes"))) === 1).as("isTherapeuticArea")
        .withColumn("leaf", size(col("children")) === 0)
        .withColumn("sources",struct(
          (col("code")).as("url"),
          col("id").as("name")
        ))
        .withColumn(
          "ontology", struct(
            (col("isTherapeuticArea")).as("isTherapeuticArea"),
            col("leaf").as("leaf"),
            col("sources").as("sources")
          )
        )
        .withColumn("children",expr("transform(children, x -> x.code)"))

      val descendants = efosSummary
        .where(size(col("ancestors")) > 0)
        // all diseases have an ancestor, at least itself
        .withColumn("ancestor", explode(concat(col("ancestors"), array(col("id")))))
        .groupBy("ancestor")
        .agg(collect_set(col("id")).as("descendants"))
        .withColumnRenamed("ancestor", "id")

      val efos= efosSummary.join(descendants, Seq("id"),"left")
        .withColumn(
          "is_id_included_descendent", array_contains(col("descendants"), col("id"))
        )

      //val included = efos.filter(col("is_id_included_descendent") === false).select("id","is_id_included_descendent").show(10, false)
      //val efoDetails = efos.select("id","label","definition","efo_synonyms")

      // Rename the columns following the graphql schema.
      val lookup = Map("label" -> "name", "definition" -> "description", "efo_synonyms" -> "synonyms", "therapeutic_codes" -> "therapeuticAreas" )
      efos.select(efos.columns.map(colname => col(colname).as(lookup.getOrElse(colname, colname))): _*)
    }

  }
}

@main
def main(relationalFilename: String,
         diseaseFilename: String,
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

  val diseases = Loaders.loadDiseases(diseaseFilename).drop("type")
  val evidences = Loaders.loadEvidences(evidenceFilename)
    .drop("evidence","id","access_level","literature","scores")
  val ddr = Loaders.loadDDR(relationalFilename).drop("id","type")


  val dfDiseases = diseases
    .setIdAndSelectFromDiseases(ss)
    .joinAll(ss,ddr,evidences)

  //println("Size diseases:"+ dfDiseases.count())
  dfDiseases.write.json(outputPathPrefix + "/diseases")
}
