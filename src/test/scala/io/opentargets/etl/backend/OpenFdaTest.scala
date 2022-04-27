package io.opentargets.etl.backend

import io.opentargets.etl.backend.openfda.stage.{
  EventsFiltering,
  PrepareBlacklistData,
  PrepareDrugList
}
import io.opentargets.etl.backend.spark.{IOResourceConfig, IOResourceConfigOption}
import io.opentargets.etl.backend.spark.IoHelpers
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class OpenFdaTest extends AnyWordSpecLike with SparkSessionSetup with Matchers {

  // Load testing data
  def dfsData = {
    val sourceData =
      Map(
        DrugData() -> IOResourceConfig(
          "json",
          this.getClass.getResource("/openfda/drug_test.json").getPath
        ),
        Blacklisting() -> IOResourceConfig(
          "csv",
          this.getClass.getResource("/openfda/blacklisted_events.txt").getPath,
          Option(
            Seq(
              IOResourceConfigOption("sep", "\\t"),
              IOResourceConfigOption("ignoreLeadingWhiteSpace", "true"),
              IOResourceConfigOption("ignoreTrailingWhiteSpace", "true")
            )
          )
        ),
        FdaData() -> IOResourceConfig(
          "json",
          this.getClass.getResource("/openfda/adverseEventSample.jsonl").getPath
        )
      )

    // Read the files
    IoHelpers.readFrom(sourceData)
  }

  "The OpenFDA FAERS ETL Stage" should {
    "successfully load only drugs of interest" in {
      val drugList = PrepareDrugList(dfsData(DrugData()).data)
      val cols = drugList.columns
      val expectedColumns = List("chembl_id", "drug_name", "linkedTargets")

      assert(cols.length == expectedColumns.length)
      assert(cols.forall(colName => expectedColumns.contains(colName)))
    }

    "properly remove blacklisted events" in {
      // Prepare Adverse Events Data
      val blackList = PrepareBlacklistData(dfsData(Blacklisting()).data)
      val fdaData = dfsData(FdaData()).data
      val fdaFilteredData = EventsFiltering(fdaData, blackList)
      assert(
        blackList
          .join(
            fdaFilteredData,
            fdaFilteredData("reaction_reactionmeddrapt") === blackList("reactions"),
            "left_anti"
          )
          .count == blackList.count
      )
    }
  }
}
