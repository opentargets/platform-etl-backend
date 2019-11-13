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


    def extractInfo(ss: SparkSession): DataFrame = {

      val dfDDR = df.withColumnRenamed("id", "relational_id")
        .withColumn("score", col("scores.overlap"))
        .withColumn("targetCountA", col("subject.links.targets_count"))
        .withColumn("targetCountB", col("object.links.targets_count"))
        .withColumn("targetCountAAndB", col("counts.shared_count"))
        .withColumn("targetCountAOrB", col("counts.union_count"))
        .withColumn("A",struct(
          (col("subject.id")).as("id"),
          col("subject.label").as("name")
        ))
        .withColumn("B",struct(
          (col("object.id")).as("id"),
          col("object.label").as("name")
        ))
        .drop("relational_id","shared_targets","type","subject", "object","counts", "shared_diseases","scores")

      dfDDR
    }

    def getDiseaseRelatedDiseases(ss: SparkSession, dfDDR: DataFrame): DataFrame = {
      val relatedSummarySource = Seq(("Open Targets","https://docs.targetvalidation.org/getting-started/scoring"))

      val dfRelatedDiseases = df.join(dfDDR, dfDDR("A.id") === col("id"), "left")
        .withColumn("relatedSummarySource",typedLit(relatedSummarySource))
        .withColumn("relatedDiseasesSingleRow", struct(col("A"), col("B"),
          col("score"), col("targetCountA"),col("targetCountB"),col("targetCountAAndB"),
          col("targetCountAOrB")))
        .withColumn("relatedSummarySource", col("relatedSummarySource").cast("""array<struct<name: String, url: String>>"""))
        .drop("A","B","score","targetCountA","targetCountB","targetCountAAndB","targetCountAOrB")

      val dfGroupRelatedDiseaseGrouped = dfRelatedDiseases.groupBy(col("id"), col("relatedSummarySource"),
        col("name"), col("description"), col("synonyms"), col("phenotypes"),
        col("therapeuticAreas"), col("phenotypesSummarySource"), col("sourcePhenotypes"),
        col("parentIds"), col("isTherapeuticArea"), col("leaf"), col("sources"),
        col("ontology"), col("descendants"))
        .agg(collect_list(col("relatedDiseasesSingleRow")).as("detailsRelatedDiseasesRows"))
        .withColumn("relatedDiseasesCount", size(col("detailsRelatedDiseasesRows")))

      val dfGroupRelatedDiseaseSummary = dfGroupRelatedDiseaseGrouped
        .withColumn("summaryrelatedDiseasesSummaries",struct(
          col("relatedDiseasesCount"),
          col("relatedSummarySource").as("sources")
        ))
        .drop("relatedDiseasesCount","relatedSummarySource")

      dfGroupRelatedDiseaseSummary

    }

    def setIdAndSelectFromDiseases(ss: SparkSession): DataFrame = {

      val getParents = udf((codes: Seq[Seq[String]]) =>
        codes.flatMap(path => if (path.size < 2) None else Some(path.reverse(1))).toSet.toSeq)
        //codes.withFilter(_.size > 1).flatMap(_.reverse(1)).toSet)

     val phenotypesSummarySource = Seq(("EFO","https://www.ebi.ac.uk/efo/"),("Orphanet","https://www.orpha.net"))

      val dfPhenotypeId =  df
          .withColumn("phenotypesSummarySource",typedLit(phenotypesSummarySource))
          .withColumn("sourcePhenotypes",
          when(size(col("phenotypes")) > 0, expr("transform(phenotypes, phRow -> named_struct('url', phRow.uri,'name',  phRow.label, 'id', substring_index(phRow.uri, '/', -1)))")))

      val efoSummaryRename = dfPhenotypeId.withColumn("phenotypesSummarySource", col("phenotypesSummarySource").cast("""array<struct<name: String, url: String>>"""))

      val efosSummary = efoSummaryRename
        .withColumn("id", substring_index(col("code"), "/", -1))
        .withColumn("ancestors", flatten(col("path_codes")))
        .withColumn("parentIds", getParents(col("path_codes")))
        .withColumn("phenotypesCount", size(col("phenotypes")))
        .drop("paths", "private", "_private", "path")
        .withColumn("phenotypes",struct(
             col("phenotypesCount"),
             col("phenotypesSummarySource").as("sources")
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

      val descendants = efosSummary
        .where(size(col("ancestors")) > 0)
        .withColumn("ancestor", explode(col("ancestors")))
        // all diseases have an ancestor, at least itself
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

    def getDiseaseDetailDrugs(ss: SparkSession, dfDiseases: DataFrame): DataFrame = {
      val drugSummarySource = Seq(("ChEMBL","https://www.ebi.ac.uk/chembl/"))

      df.printSchema()

      val dfDrugsExtended = df
        .where(col("`type`") === "known_drug")
        .withColumn("disease_id", col("disease.id"))
        .withColumn("drug_id", substring_index(col("drug.id"), "/", -1))
        .join(dfDiseases, expr("array_contains(descendants,disease_id)"), "inner")

      val dfDrugDetails = dfDrugsExtended.groupBy(col("id"),
        col("disease"),col("target"),col("drug"),
        col("drug_id"), col("mechanism_of_action"), col("clinicalTrial"))
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
        .withColumn("EvidenceRowDrugs_single_row", struct(col("clinicalTrial"),
          col("mechanism_of_action"),
          col("drug_details").as("drug"), col("target_details").as("target"),
          col("disease_details").as("disease")
        ))
        .drop("drug_details","target_details","disease_details","mechanism_of_action", "clinicalTrial")

      val dfUnionInfo = dfDrugDetails.join(dfDiseases, Seq("id"),"right")

      val dfEvidenceRows = dfUnionInfo.groupBy(col("id"),col("ontology"), col("phenotypes"),
        col("name"),col("sourcePhenotypes"), col("detailsRelatedDiseasesRows"),
        col("description"), col("therapeuticAreas"), col("parentIds"),col("synonyms"),
        col("summaryrelatedDiseasesSummaries"))
        .agg(collect_set(col("drug_id")).as("associated_drugs"),
          collect_list(col("EvidenceRowDrugs_single_row")).as("drugs_rows"))
        .withColumn("drugCount", size(col("associated_drugs")))
        .withColumn("drugSummarySource",typedLit(drugSummarySource).cast("""array<struct<name: String, url: String>>"""))
        .withColumn("drugs",struct(
          (col("drugCount")),col("drugSummarySource").as("sources"))
        ).drop("drugCount","drugsSources")
        .withColumn("summaries",
             struct(
                 col("ontology"),
                 col("phenotypes"),
                 col("drugs")))
        .drop("ontology","phenotypes","drugs","associated_drugs","drugsSources","drugCount","drugSummarySource")
        .withColumn("summaries", // <-- use the same column name so you hide the existing one
          struct(
            col("summaries.phenotypes"), // <-- reference existing column to copy the values
            col("summaries.ontology"),
            col("summaries.drugs"),
            col("summaryrelatedDiseasesSummaries").as("relatedDiseases"))) // <-- new field
        .drop("summaryrelatedDiseasesSummaries")
        .withColumn("drugs_rows_alias", struct(col("drugs_rows").as("rows")))
        .withColumn("relatedDiseases_rows_alias", struct(col("detailsRelatedDiseasesRows").as("rows")))
        .withColumn("phenotypes_rows_alias", struct(col("sourcePhenotypes").as("rows")))
        .drop("drugs_rows","sourcePhenotypes","detailsRelatedDiseasesRows")
        .withColumn("details",
          struct(
            col("drugs_rows_alias").as("drugs"),
            col("phenotypes_rows_alias").as("phenotypes"),
            col("relatedDiseases_rows_alias").as("relatedDiseases")
          )
        )
        .drop("relatedDiseases_rows_alias","phenotypes_rows_alias","drugs_rows_alias")


      //println(dfEvidenceRows.filter(col("id")==="EFO_0000311").show())
      dfEvidenceRows
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

  val diseases = Loaders.loadDiseases(diseaseFilename)
  val evidences = Loaders.loadEvidences(evidenceFilename)
  val ddr = Loaders.loadDDR(relationalFilename)

  val dfDiseases = diseases
    .setIdAndSelectFromDiseases(ss)

  //val listTherapeuticAreas = all_diseases.filter(col("isTherapeuticArea") === true).select("id")
  //println(dfDiseases.filter(col("id") === "Orphanet_262").show(false))
  //println("Diseases"+ dfDiseases.count())
  //println("Evidences"+ evidences.count())
  //println("DDR"+ ddr.count())

  val dfDDR = ddr.extractInfo(ss)
  // efo3 index left join relatedDisease
  val dfDiseaseRelatedDiseases= dfDiseases.getDiseaseRelatedDiseases(ss,dfDDR)
  println("Diseases+DDR"+ dfDiseaseRelatedDiseases.count())
  dfDiseaseRelatedDiseases.printSchema()

  val dfEvidencesBase = evidences
    .withColumn("mechanism_of_action",
      when(col("evidence.target2drug.mechanism_of_action").isNotNull, col("evidence.target2drug.mechanism_of_action")).otherwise(null))
    .withColumn("clinicalTrial",when(col("evidence.drug2clinic.clinical_trial_phase").isNotNull,
      struct(col("evidence.drug2clinic.clinical_trial_phase.numeric_index").as("phase"),
        col("evidence.drug2clinic.status").as("status"),
        col("evidence.drug2clinic.urls.url").getItem(0).as("sourceUrl"),
        col("evidence.drug2clinic.urls.nice_name").getItem(0).as("sourceName"))).otherwise(null))
  .drop("evidence","id","access_level","literature")

  //dfEvidencesBase.filter(col("disease.id") === "EFO_0000384").select("disease.id","mechanism_of_action","clinicalTrial").show(100,false)

  val dfSummariesAndDetails = dfEvidencesBase.getDiseaseDetailDrugs(ss, dfDiseaseRelatedDiseases)

  println("Size diseases:"+ dfSummariesAndDetails.count())
  dfSummariesAndDetails.write.json(outputPathPrefix + "/diseases")

}
