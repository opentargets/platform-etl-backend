package io.opentargets.etl.backend

import io.opentargets.etl.backend.openfda.stage.PrepareDrugList
import io.opentargets.etl.backend.spark.{IOResourceConfig, IOResourceConfigOption}
import io.opentargets.etl.backend.spark.IoHelpers
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class OpenFdaTest
    extends AnyWordSpecLike
//    with PrivateMethodTester
    with SparkSessionSetup
    with Matchers {

  "The OpenFDA FAERS ETL Stage" should {
    // Load testing data
    val sourceData =
      Map(
        DrugData() -> IOResourceConfig("json",
                                       this.getClass.getResource("/drug_test.json").getPath),
        Blacklisting() -> IOResourceConfig(
          "csv",
          this.getClass.getResource("/blacklisted_events.txt").getPath,
          Option(
            Seq(
              IOResourceConfigOption("sep", "\\t"),
              IOResourceConfigOption("ignoreLeadingWhiteSpace", "true"),
              IOResourceConfigOption("ignoreTrailingWhiteSpace", "true")
            ))
        ),
        FdaData() -> IOResourceConfig(
          "json",
          this.getClass.getResource("/adverseEventSample.jsonl").getPath)
      )
    // Read the files
    val dfsData = IoHelpers.readFrom(sourceData)

    "successfully load only drugs of interest" in {
      val drugList = PrepareDrugList(dfsData(DrugData()).data)
      val cols = drugList.columns
      val expectedColumns = List("chembl_id", "drug_name")

      assert(cols.length == expectedColumns.length)
      assert(cols.forall(colName => expectedColumns.contains(colName)))
    }
  }

  "properly remove blacklisted events" in {

  }

}
