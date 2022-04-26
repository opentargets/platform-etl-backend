package io.opentargets.etl.backend.Graph

import io.opentargets.etl.backend.EtlSparkUnitTest
import io.opentargets.etl.backend.graph
import io.opentargets.etl.backend.graph.GraphNode
import org.apache.spark.sql.functions.{array_contains, col, map_keys}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object GraphNodeTest {

  def createDF(data: Seq[(String, String)], structure: Seq[String])(implicit
      sparkSession: SparkSession
  ): DataFrame = {
    val dataframe = sparkSession.createDataFrame(data).toDF(structure: _*)
    dataframe
  }

}

class GraphNodeTest extends EtlSparkUnitTest {
  import sparkSession.implicits._

  "Processing two Dataframe " should "return a dataframe with parents,children, ancestors and descendants" in {
    // given
    val efoStruct = Seq("id", "name")
    val efoData = Seq(
      ("MONDO_0002135", "optic nerve disease"),
      ("EFO_0003957", "radius fracture"),
      ("EFO_0007357", "mansonelliasis"),
      ("EFO_0008621", "Microcystic Renal Disease"),
      ("EFO_0003333", "Fake disease")
    )

    val vertices = GraphNodeTest.createDF(efoData, efoStruct)(sparkSession)

    val edgeData = Seq(
      ("MONDO_0002135", "EFO_0003957"),
      ("EFO_0003957", "EFO_0007357"),
      ("EFO_0007357", "EFO_0008621"),
      ("EFO_0007357", "EFO_0003333")
    )
    val edgeStruct = Seq("src", "dst")
    val edges = GraphNodeTest.createDF(edgeData, edgeStruct)(sparkSession)

    val expectedColumns =
      Set("id", "label", "ancestors", "descendants", "children", "parents", "path")

    // when
    val ancestry = GraphNode(vertices, edges)

    // then
    assert(expectedColumns.forall(expectedCol => ancestry.columns.contains(expectedCol)))
    assert(ancestry.count() == vertices.count())
  }

  "Processing two empty Dataframe " should "return an empty dataframe with parents,children, ancestors and descendants" in {
    // given
    val efoStruct = Seq("id", "name")
    val efoData = Seq()

    val vertices = GraphNodeTest.createDF(efoData, efoStruct)(sparkSession)
    val edgeStruct = Seq("src", "dst")
    val edgeData = Seq()

    val edges = GraphNodeTest.createDF(edgeData, edgeStruct)(sparkSession)

    val expectedColumns =
      Set("id", "label", "ancestors", "descendants", "children", "parents", "path")

    // when
    val ancestry = GraphNode(vertices, edges)

    // then
    assert(expectedColumns.forall(expectedCol => ancestry.columns.contains(expectedCol)))
    assert(ancestry.count() == 0)
  }

}
