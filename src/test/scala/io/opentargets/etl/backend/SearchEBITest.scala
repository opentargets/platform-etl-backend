package io.opentargets.etl.backend

import io.opentargets.etl.backend.spark.{IOResource, IOResourceConfig}
import org.apache.spark.sql.SparkSession
import io.opentargets.etl.backend.spark.IoHelpers.IOResources

object SearchEBITest {

  def setupResources(implicit sparkSession: SparkSession): IOResources = {

    val inputTargets = Seq(
      ("ENSG00000157764", "BRAF"),
      ("ENSG00000171862", "PTEN"),
      ("ENSG00000139618", "BRCA2"),
      ("ENSG00000142192", "APP")
    )
    val targets = sparkSession.createDataFrame(inputTargets).toDF("id", "approvedSymbol")

    val inputDiseases = Seq(
      ("EFO_0003767", "inflammatory bowel disease"),
      ("EFO_0000729", "ulcerative colitis"),
      ("EFO_0000692", "schizophrenia")
    )
    val diseases = sparkSession.createDataFrame(inputDiseases).toDF("id", "name")

    val inputEvidence = Seq(
      ("ENSG00000157764", "EFO_0003767", 0.78778),
      ("ENSG00000139618", "EFO_0000692", 0.98987),
      ("ENSG00000171862", "EFO_0000729", 0.4323)
    )
    val evidence =
      sparkSession.createDataFrame(inputEvidence).toDF("targetId", "diseaseId", "score")

    val inputAssociations = Seq(
      ("ENSG00000171862", "EFO_0003767", 0.8737),
      ("ENSG00000139618", "EFO_0000692", 0.5555),
      ("ENSG00000171862", "EFO_0000729", 0.3232)
    )
    val associations =
      sparkSession
        .createDataFrame(inputAssociations)
        .toDF("targetId", "diseaseId", "associationScore")

    val config = IOResourceConfig("csv", "")
    val allResources: IOResources =
      Map(
        "target" -> IOResource(targets, config),
        "disease" -> IOResource(diseases, config),
        "evidence" -> IOResource(evidence, config),
        "association" -> IOResource(associations, config)
      )
    allResources
  }
}

class SearchEBITest extends EtlSparkUnitTest {

  "Processing Diseases,Target and Evidence" should "return a dataframe with a specific list of attributes" in {
    // given
    val resources: IOResources = SearchEBITest.setupResources(sparkSession)
    val expectedColumns = Set("diseaseId", "targetId", "score", "approvedSymbol", "name")
    // when
    val results = SearchEBI.generateDatasets(resources)

    // then
    assert(
      expectedColumns.forall(expectedCol =>
        results("ebisearchEvidence").columns.contains(expectedCol)
      )
    )

    assert(
      expectedColumns.forall(expectedCol =>
        results("ebisearchAssociations").columns.contains(expectedCol)
      )
    )
  }

}
