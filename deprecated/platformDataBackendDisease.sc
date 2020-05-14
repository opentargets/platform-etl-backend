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
}

object Transformers {
  implicit class Implicits (val df: DataFrame) {

    def setIdAndSelectFromDiseases(ss: SparkSession): DataFrame = {

      val getParents = udf((codes: Seq[Seq[String]]) =>
        codes.flatMap(path => if (path.size < 2) None else Some(path.reverse(1))).toSet.toSeq)
      //codes.withFilter(_.size > 1).flatMap(_.reverse(1)).toSet)

      val dfPhenotypeId =  df
        .withColumn("sourcePhenotypes",
          when(size(col("phenotypes")) > 0, expr("transform(phenotypes, phRow -> named_struct('url', phRow.uri,'name',  phRow.label, 'disease', substring_index(phRow.uri, '/', -1)))")))

      import ss.implicits._

      val efosSummary = dfPhenotypeId
        .withColumn("id", substring_index(col("code"), "/", -1))
        .withColumn("ancestors",
          array_except(array_distinct(flatten(col("path_codes"))),
            array(col("id"))) )
        .withColumn("parents", getParents(col("path_codes")))
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
      // Change the value of children from array struct to array of code.
      .withColumn("children", expr("transform(children, child -> child.code)"))

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

      val efosRenamed = efos.withColumnRenamed("label","name")
          .withColumnRenamed("definition" , "description")
          .withColumnRenamed( "efo_synonyms", "synonyms")
          .withColumnRenamed("therapeutic_codes","therapeuticAreas")
          .drop("definition_alternatives","path_codes", "isTherapeuticArea", "leaf","path_labels",
            "therapeutic_labels","sources","phenotypesCount","sourcePhenotypes")

      efosRenamed

    }

  }
}

@main
def main(diseaseFilename: String, outputPathPrefix: String): Unit = {
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

  val dfDiseases = diseases
    .setIdAndSelectFromDiseases(ss)

  dfDiseases.printSchema()
  //println("Size diseases:"+ dfDiseases.count())
  dfDiseases.write.json(outputPathPrefix + "/diseases")
}
