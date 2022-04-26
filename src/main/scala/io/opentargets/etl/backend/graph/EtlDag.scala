package io.opentargets.etl.backend.graph

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.Configuration.EtlStep
import io.opentargets.etl.backend.graph.GraphNode.{DAGT, makeGraph}
import org.jgrapht.graph.DefaultEdge
import org.jgrapht.traverse.TopologicalOrderIterator

import scala.annotation.tailrec
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

class EtlDag[V](dagConfig: List[EtlStep[V]]) extends LazyLogging {

  lazy val getAll: List[V] = {
    logger.info("Generating DAG for all steps.")
    val ts = new TopologicalOrderIterator[V, DefaultEdge](graph)

    val dag = getStepsFromIter(ts).reverse
    logger.info(s"DAG for all steps: $dag")
    dag
  }
  private val graph: DAGT[V] = makeGraph(
    dagConfig.map(_.step),
    dagConfig.foldLeft(Seq.empty[(V, V)])((acc, nxt) => {
      val src = nxt.step
      val dests = nxt.dependencies
      // If a step has no dependencies we want to give it a 'dummy' parent so we always have a connected graph.
      dests match {
        case noDeps if noDeps.isEmpty => acc
        case _                        => acc ++ dests.map((_, src))
      }
    })
  )

  def getDependenciesFor(step: V*): List[V] = {
    logger.info(s"Resolving dependencies for $step")

    @tailrec
    def go(requestedSteps: Seq[V], stepsToExecute: List[V] = List.empty[V]): List[V] = {
      if (requestedSteps.isEmpty) stepsToExecute
      else {
        val steps = getDependencies(requestedSteps.head)
        go(requestedSteps.tail, steps ++ stepsToExecute)
      }
    }

    val dag = go(step).distinct
    logger.info(s"DAG for $step: $dag")
    dag
  }

  @tailrec
  private def getStepsFromIter(
      iter: => TopologicalOrderIterator[V, DefaultEdge],
      agg: List[V] = List.empty[V]
  ): List[V] = {
    if (!iter.hasNext) agg else getStepsFromIter(iter, iter.next :: agg)
  }

  private def getDependencies(step: V): List[V] = {
    try {
      val ts = new TopologicalOrderIterator[V, DefaultEdge](graph)
      val ancestors = step :: graph.getAncestors(step).toList
      getStepsFromIter(ts).reverse.filter(ancestors.contains)
    } catch {
      case e: IllegalArgumentException =>
        logger.error(
          s"Unable to find node $step in ETL DAG with nodes ${graph.vertexSet().toString}"
        )
        List.empty
    }
  }
}
