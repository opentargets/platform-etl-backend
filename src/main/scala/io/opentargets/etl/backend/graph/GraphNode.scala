package io.opentargets.etl.backend.graph

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.collection.convert.ImplicitConversions._
import org.jgrapht._
import org.jgrapht.generate._
import org.jgrapht.traverse._
import org.jgrapht.graph._
import org.jgrapht.Graphs._
import org.jgrapht.util._
import org.jgrapht.alg.shortestpath._

/**
  vertices must contain "id", "label" fields. The field label can be called in a flexible way.
  edges must contain "src" and "dst" fields
  * */
object GraphNode extends Serializable with LazyLogging {

  case class GraphNodeDocument(id: String,
                               label: String,
                               ancestors: Seq[String],
                               descendants: Seq[String],
                               children: Seq[String],
                               parents: Seq[String],
                               path: Seq[Seq[String]])

  type DAGT = DirectedAcyclicGraph[String, DefaultEdge]

  /* Given two dataframe vertices[id,label] and edges[src,dst] this method build a graph */
  def makeGraph(vertices: DataFrame, edges: DataFrame): DAGT = {

    val jgraph =
      new org.jgrapht.graph.DirectedAcyclicGraph[String, DefaultEdge](classOf[DefaultEdge])

    vertices.collect.foreach(r => jgraph.addVertex(r.getAs[String]("id")))
    edges.collect.foreach(r => {
      try {
        jgraph.addEdge(r.getAs[String]("src"), r.getAs[String]("dst"))
      } catch {
        case _ =>
      }
    })

    jgraph
  }

  /** given the graph and the vertices(id,label) it generates a dataframe with id, parents, children, ... */
  def processGraph(vertices: DataFrame, graph: DAGT)(implicit ss: SparkSession): DataFrame = {
    import ss.implicits._

    logger.debug("Compute the graph. Calculate the ancestry.")
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

  def apply(vertices: DataFrame, edges: DataFrame)(implicit ss: SparkSession): DataFrame = {

    val graph = makeGraph(vertices, edges)
    val ancestry = processGraph(vertices, graph)

    ancestry
  }

}
