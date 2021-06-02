package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import io.opentargets.etl.backend.spark.{IOResource, IOResourceConfig, IoHelpers}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources

object CancerBiomarkersHelpers extends LazyLogging {
  implicit class AggregationHelpers(df: DataFrame)(implicit ss: SparkSession) {
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
          col("details.individualbiomarker") as "individualbiomarker",
          col("details.biomarkerId") as "biomarkerId",
          col("details.drugName") as "drugName",
          col("details.associationType") as "associationType",
          col("details.evidenceLevel") as "evidenceLevel",
          col("details.sourcesPubmedRoot") as "sourcesPubmedRoot",
          col("details.sourcesOtherRoot") as "sourcesOtherRoot",
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
  def apply()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession
    import CancerBiomarkersHelpers._

    val common = context.configuration.common
    val name = "cancerBiomarker"
    val targetDFName = "target"
    val mappedInputs = Map(
      targetDFName -> IOResourceConfig(common.inputs.target.format, common.inputs.target.path)
    )
    val targets = IoHelpers.readFrom(mappedInputs).apply(targetDFName).data
    val inputDataFrame = IoHelpers.readFrom(mappedInputs)

    val cancerBiomarkerConf = IOResourceConfig(
      context.configuration.common.outputFormat,
      context.configuration.common.output + s"/$name"
    )

    val cancerBiomakerDf = targets.getBiomarkerTargetDiseaseDrugEntity
    val outputs = Map(
      name -> IOResource(cancerBiomakerDf, cancerBiomarkerConf)
    )

    IoHelpers.writeTo(outputs)
  }
}
