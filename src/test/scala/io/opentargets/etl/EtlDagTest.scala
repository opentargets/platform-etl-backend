package io.opentargets.etl

import io.opentargets.etl.backend.Configuration
import io.opentargets.etl.backend.Configuration.{EtlStep, OTConfig}
import io.opentargets.etl.backend.graph.EtlDag
import org.scalatest.{AppendedClues, Assertion}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import pureconfig.ConfigReader

class EtlDagTest extends AnyFlatSpecLike with Matchers with AppendedClues {

  /** root
    * /
    * A
    * /  \
    * D   E
    * /
    * C
    * /
    * B
    */
  val testGraphConf = List(
    EtlStep("A", List()),
    EtlStep("B", List("C", "D")),
    EtlStep("C", List("D")),
    EtlStep("D", List("A")),
    EtlStep("E", List("A"))
  )
  val dag = new EtlDag[String](testGraphConf)

  // Using ETL Configuration from `reference.conf`
  val conf: ConfigReader.Result[OTConfig] = Configuration.config
  val dagETL = new EtlDag[String](conf match {
    case Left(_)  => testGraphConf
    case Right(c) => c.etlDag.steps
  })
  "An ETL DAG" should "contain a node for each step" in {
    dag.getAll.length should be(testGraphConf.length)
  }

  it should "return all required steps and the selected step" in {
    dag.getDependenciesFor("D") should contain inOrderOnly ("A", "D")
  }

  it should "return the steps such that ancestors are before children" in {
    dag.getDependenciesFor(
      "C",
      "E"
    ) should (contain inOrderOnly ("A", "E", "D", "C") and have length 4)
  }

  "The Open Targets DAG" should "correctly resolve for complex step ('search')" in {
    // given
    val step = "search"
    // when
    implicit val stepsInOrder: Seq[String] = dagETL.getDependenciesFor(step)

    // then
    stepsInOrder.last should be(step) // since all its dependencies must run before it
    // 'evidence' must run after 'target'
    aBeforeB("target", "evidence")
    aBeforeB("target", "drug")
    aBeforeB("target", "association")
  }

  it should "correctly resolve for simple step" in {
    // given
    val step = "reactome"
    // when
    implicit val stepsInOrder: Seq[String] = dagETL.getDependenciesFor(step)
    // then
    stepsInOrder.last should be(step) // since all its dependencies must run before it
    stepsInOrder should have size 1 // since it has no dependencies
  }

  it should "correctly resolve for all steps" in {
    // when
    implicit val stepsInOrder: Seq[String] = dagETL.getAll
    aBeforeB("evidence", "association")
    aBeforeB("drug", "knownDrugs")
    aBeforeB("reactome", "target")
    aBeforeB("target", "drug")
    aBeforeB("target", "fda")
    aBeforeB("targetValidation", "knownDrug")
    aBeforeB("target", "evidence")
    aBeforeB("association", "search")
    aBeforeB("disease", "evidence")
  }

  private def aBeforeB(a: String, b: String)(implicit steps: Seq[String]): Assertion = {
    val first = steps.zipWithIndex
      .filter(it => it._1 == a || it._1 == b)
      .minBy(_._2)
      ._1
    first should be(a) withClue "and the ETL will fail as the dependencies are in the wrong order."
  }

}
