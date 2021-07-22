package io.opentargets.etl.preprocess

import io.opentargets.etl.preprocess.GoConverterTest.testInput
import io.opentargets.etl.preprocess.go.GoConverter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

object GoConverterTest {

  val testInput: Seq[String] =
    """
      |property_value: http://purl.org/dc/elements/1.1/title "Gene Ontology" xsd:string
      |property_value: http://purl.org/dc/terms/license http://creativecommons.org/licenses/by/4.0/
      |property_value: owl:versionInfo "2021-07-02" xsd:string
      |
      |[Term]
      |id: GO:0000001
      |name: mitochondrion inheritance
      |namespace: biological_process
      |def: "The distribution of mitochondria, including the mitochondrial genome, into daughter cells after mitosis or meiosis, mediated by interactions between mitochondria and the cytoskeleton." [GOC:mcc, PMID:10873824, PMID:11389764]
      |synonym: "mitochondrial inheritance" EXACT []
      |is_a: GO:0048308 ! organelle inheritance
      |is_a: GO:0048311 ! mitochondrion distribution
      |
      |[Term]
      |id: GO:0000002
      |name: mitochondrial genome maintenance
      |namespace: biological_process
      |def: "The maintenance of the structure and integrity of the mitochondrial genome; includes replication and segregation of the mitochondrial chromosome." [GOC:ai, GOC:vw]
      |is_a: GO:0007005 ! mitochondrion organization
      |
      |[Typedef]
      |id: term_tracker_item
      |name: term tracker item
      |namespace: external
      |xref: IAO:0000233
      |is_metadata_tag: true
      |is_class_level: true
      |
      |""".stripMargin.split("\n")
}

class GoConverterTest extends AnyFlatSpec with Matchers {
  "Raw obo file" should "be parsed into a collection of GO objects" in {
    // when
    val results = GoConverter.convertFileToGo(testInput.toIterator)

    // then
    results.size should be(2)
    results.head.id should be("GO:0000001")
    results.head.name should be("mitochondrion inheritance")
  }
}
