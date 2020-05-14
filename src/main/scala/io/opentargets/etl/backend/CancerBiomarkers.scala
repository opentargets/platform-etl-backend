package io.opentargets.etl.backend

import org.apache.spark.SparkConf
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.typesafe.config.Config
import io.opentargets.etl.backend.SparkHelpers.IOResourceConfig

object CancerBiomarkersHelpers {
  implicit class AggregationHelpers(df: DataFrame)(implicit ss: SparkSession) {
    import Configuration._
    import ss.implicits._

    def getBiomarkerTargetDiseaseDrugEntity: DataFrame = {

      val selectExpressions = Seq(
        "id",
        "cancerbiomarkers as cancerBiomarkers"
      )

      val dfBiomarkersInput = df.selectExpr(selectExpressions: _*).withColumnRenamed("id", "target")

      val dfExtractInfo = dfBiomarkersInput
        .withColumn(
          "cancerBiomarkersDetails",
          when(
            size(col("cancerBiomarkers")) > 0,
            expr(
              "transform(cancerBiomarkers, bioRow -> named_struct('individualbiomarker',bioRow.individualbiomarker,'biomarkerId', bioRow.biomarker,'diseases', bioRow.diseases,'drugName',bioRow.drugfullname,'associationType',bioRow.association, 'evidenceLevel', bioRow.evidencelevel,'sourcesOtherRoot', bioRow.references.other, 'sourcesPubmedRoot', bioRow.references.pubmed))"
            )
          )
        )
        .withColumn("details", explode(col("cancerBiomarkersDetails")))
        .drop("cancerBiomarkersDetails")
        .groupBy(
          col("details.individualbiomarker"),
          col("details.biomarkerId"),
          col("details.drugName"),
          col("details.associationType"),
          col("details.evidenceLevel"),
          col("details.sourcesPubmedRoot"),
          col("details.sourcesOtherRoot"),
          col("target")
        )
        .agg(collect_list("details.diseases").as("diseasesNested"))
        .withColumn("diseases", flatten(col("diseasesNested")))
        .drop("diseasesNested")
        .withColumn("disease", explode(col("diseases.id")))
        .withColumn(
          "sourcesPubmed",
          when(
            size(col("sourcesPubmedRoot")) > 0,
            expr(
              "transform(sourcesPubmedRoot, srcPub -> cast(srcPub.pmid AS LONG))"
            )
          )
        )
        .withColumn(
          "sourcesOther",
          when(size(col("sourcesOtherRoot")) > 0, col("sourcesOtherRoot"))
        )

      /** The field individualbiomarker contains a specific fields if the biomarker id is a composed id.
		  It is important to idenfity the unique identifier id.
		  Below the id is the proper identifier
		**/
      val biomarkerIdentifier =
        """
          |case
          |  when (individualbiomarker = '' or individualbiomarker = null) then biomarkerId
          |  else individualbiomarker
          |end as id
          |""".stripMargin

      val selectExpressionBiomarkers =
        Seq(
          "individualbiomarker",
          "biomarkerId",
          "drugName",
          "target",
          "disease",
          "evidenceLevel",
          "associationType",
          "sourcesPubmed",
          "sourcesOther"
        )

      val dfBiomarkers =
        dfExtractInfo
          .selectExpr(selectExpressionBiomarkers :+ biomarkerIdentifier: _*)
          .drop("biomarkerId", "individualbiomarker")

      dfBiomarkers

    }
  }
}

// This is option/step cancerbiomarkers in the config file
object CancerBiomarkers extends LazyLogging {
  def apply()(implicit context: ETLSessionContext) = {
    implicit val ss = context.sparkSession
    import ss.implicits._
    import CancerBiomarkersHelpers._

    val common = context.configuration.common
    val mappedInputs = Map(
      "target" -> IOResourceConfig(common.inputs.target.format, common.inputs.target.path)
    )
    val inputDataFrame = SparkHelpers.readFrom(mappedInputs)

    val cancerBiomakerDf = inputDataFrame("target").getBiomarkerTargetDiseaseDrugEntity

    val outputs = Seq("cancerBiomarkers")

    // TODO THIS NEEDS MORE REFACTORING WORK AS IT CAN BE SIMPLIFIED
    val outputConfs = outputs
      .map(
        name =>
          name -> IOResourceConfig(context.configuration.common.outputFormat,
                                   context.configuration.common.output + s"/$name"))
      .toMap

    val outputDFs = (outputs zip Seq(cancerBiomakerDf)).toMap
    SparkHelpers.writeTo(outputConfs, outputDFs)
  }
}
