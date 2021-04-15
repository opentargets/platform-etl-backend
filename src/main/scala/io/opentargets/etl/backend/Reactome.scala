package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import io.opentargets.etl.backend.spark.Helpers
import io.opentargets.etl.backend.spark.Helpers.{IOResource, IOResourceConfig, IOResources}
import scala.collection.convert.ImplicitConversions._
import org.jgrapht._
import org.jgrapht.generate._
import org.jgrapht.traverse._
import org.jgrapht.graph._
import org.jgrapht.Graphs._
import org.jgrapht.util._
import org.jgrapht.alg.shortestpath._

// This is option/step reactome in the config file
object Reactome extends LazyLogging {

  type DAGT = DirectedAcyclicGraph[String, DefaultEdge]
  case class GraphNodeDocument(id: String,
                               label: String,
                               ancestors: Seq[String],
                               descendants: Seq[String],
                               children: Seq[String],
                               parents: Seq[String],
                               path: Seq[Seq[String]])

  def cleanPathways(df: DataFrame): DataFrame =
    df.filter(col("_c2") === "Homo sapiens")
      .drop("_c2")
      .toDF("id", "name")

  def makeGraph(pathways: DataFrame, relations: DataFrame): DAGT = {
    val vertices = pathways

    val edges = relations
      .toDF("src", "dst")

    val jg = new org.jgrapht.graph.DirectedAcyclicGraph[String, DefaultEdge](classOf[DefaultEdge])

    vertices.collect.foreach(r => jg.addVertex(r.getAs[String]("id")))
    edges.collect.foreach(r => {
      try {
        jg.addEdge(r.getAs[String]("src"), r.getAs[String]("dst"))
      } catch {
        case _: Throwable =>
      }
    })

    jg

  }

  def processGraph(vertices: DataFrame, graph: DAGT)(
      implicit context: ETLSessionContext): DataFrame = {
    import context.sparkSession.implicits._

    logger.debug("compute the graph")
    val topV = graph.vertexSet.filter(p => graph.inDegreeOf(p) == 0).toSeq
    val adp = new AllDirectedPaths(graph)

    val V = vertices.collectAsList.map { r =>
      val id = r.getString(0)
      val label = r.getString(1)
      val paths = adp
        .getAllPaths(topV.toSet, Set(id), true, null)
        .toSeq
        .map(e => e.getVertexList.toSeq)
      GraphNodeDocument(id,
                        label,
                        graph.getAncestors(id).toSeq,
                        graph.getDescendants(id).toSeq,
                        successorListOf(graph, id),
                        predecessorListOf(graph, id),
                        paths)
    }

    V.toDF
  }

  def apply()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession
    val dfName = "reactome"
    val reactomeC = context.configuration.reactome

    val mappedInputs = Map(
      "pathways" -> reactomeC.inputs.pathways,
      "relations" -> reactomeC.inputs.relations
    )

    val reactomeIs = Helpers.readFrom(mappedInputs)
    val pathways = reactomeIs("pathways").data.transform(cleanPathways)
    val g = makeGraph(pathways, reactomeIs("relations").data)
    val index = processGraph(pathways, g)

    logger.info("compute reactome dataset")
    val outputs = Map(
      dfName -> IOResource(index, reactomeC.output)
    )

    Helpers.writeTo(outputs)
  }
}
