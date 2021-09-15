package io.opentargets.etl.preprocess

import better.files.File
import io.opentargets.etl.preprocess.uniprot.{
  CommentIdentifiers,
  DbIdentifiers,
  DescriptionIdentifiers,
  UniprotConverter,
  UniprotEntry,
  UniprotEntryParsed
}
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

trait UniprotConverterTestInputs {
  lazy val oneEntry: Iterator[String] = File(
    this.getClass.getResource("/uniprot/sample_1.txt").getPath).lineIterator
  lazy val tenEntries: Iterator[String] = File(
    this.getClass.getResource("/uniprot/sample_10.txt").getPath).lineIterator
}

class UniprotConverterTest
    extends AnyFlatSpec
    with Matchers
    with PrivateMethodTester
    with TableDrivenPropertyChecks
    with DbIdentifiers
    with DescriptionIdentifiers
    with CommentIdentifiers
    with UniprotConverterTestInputs {

  "The UniprotConverter" should "convert from a flat file to case classes" in {
    // given one entry
    // when
    val entries: List[UniprotEntryParsed] =
      UniprotConverter.convertUniprotFlatFileToUniprotEntry(oneEntry)
    // then
    entries should have size 1
    // and all database entries should be of interest
    entries.head.dbXrefs
      .forall(d => dbOfInterest.exists(_.startsWith(d.split(POST_PROCESS_SPLIT).head))) should be(
      true)
  }

  "The ID" should "be extracted from the raw line starting with ID" in {
    // given
    val idLine = "ID  PRS6A_HUMAN             Reviewed;         439 AA."
    val extractId = PrivateMethod[String](Symbol("processId"))
    // when
    val result = UniprotConverter invokePrivate extractId(idLine)
    // then
    result should equal("PRS6A_HUMAN")
  }

  "Descriptions" should "be converted to lists of recommended and alternative names" in {
    //given
    val input = Seq(
      "RecName: Full=CD5 antigen-like;",
      "AltName: Full=Apoptosis inhibitor expressed by macrophages {ECO:0000303|PubMed:23236605}; ",
      "          Short=hAIM {ECO:0000303|PubMed:23236605};",
      "AltName: Full=CT-2 {ECO:0000303|Ref.2};",
      "AltName: Full=IgM-associated peptide {ECO:0000303|PubMed:8034987};",
      "AltName: Full=SP-alpha {ECO:0000303|PubMed:9045627};",
      "Flags: Precursor;"
    )

    // when
    val result = processNames(input)
    // then
    result.recNames should have size 1
    result.altNames should have size 4
    result.symbols should have size 1
    result.recNames should contain theSameElementsAs Seq("CD5 antigen-like")
    result.altNames should contain theSameElementsAs Seq(
      "Apoptosis inhibitor expressed by macrophages",
      "CT-2",
      "IgM-associated peptide",
      "SP-alpha")
    result.symbols should contain theSameElementsAs Seq("hAIM")
  }

  "Comments" should "be correctly partitioned into functions and subcellular locations" in {
    // given
    val commentsRaw = Seq(
      "-!- FUNCTION: [Isoform 3]: Cleaves GlcNAc but not GalNAc from O-",
      "glycosylated proteins. Can use p-nitrophenyl-beta-GlcNAc as substrate",
      "but not p-nitrophenyl-beta-GalNAc or p-nitrophenyl-alpha-GlcNAc (in",
      "vitro), but has about six times lower specific activity than isoform 1.",
      "-!- CATALYTIC ACTIVITY:",
      "Reaction=3-O-(N-acetyl-beta-D-glucosaminyl)-L-seryl-[protein] + H2O =",
      "L-seryl-[protein] + N-acetyl-D-glucosamine; Xref=Rhea:RHEA:48876,",
      "Rhea:RHEA-COMP:9863, Rhea:RHEA-COMP:12251, ChEBI:CHEBI:15377,",
      "ChEBI:CHEBI:29999, ChEBI:CHEBI:90838, ChEBI:CHEBI:506227;",
      "EC=3.2.1.169; Evidence={ECO:0000269|PubMed:11148210,",
      "ECO:0000269|PubMed:11788610, ECO:0000269|PubMed:18586680,",
      "ECO:0000269|PubMed:20863279, ECO:0000269|PubMed:22365600,",
      "ECO:0000305|PubMed:20673219};",
      "-!- SUBCELLULAR LOCATION: [Isoform 3]: Nucleus",
      "{ECO:0000269|PubMed:11341771}.",
      "-!- SUBCELLULAR LOCATION: [Isoform 1]: Cytoplasm",
      "{ECO:0000269|PubMed:11148210, ECO:0000269|PubMed:11341771}",
      "-!- SUBCELLULAR LOCATION: Cell projection, cilium, photoreceptor outer",
      "segment {ECO:0000269|PubMed:27613864}. Membrane",
      "{ECO:0000269|PubMed:27613864}; Lipid-anchor",
      "{ECO:0000269|PubMed:27613864}; Cytoplasmic side",
      "{ECO:0000250|UniProtKB:Q00LT2}. Endoplasmic reticulum",
      "{ECO:0000269|PubMed:24992209}. Golgi apparatus",
      "{ECO:0000269|PubMed:24992209}. Note=Localizes to photoreceptor disk",
      "membranes in the photoreceptor outer segment (PubMed:27613864). The",
      "secretion in media described in PubMed:24992209 is probably an",
      "experimental artifact (PubMed:24992209). {ECO:0000269|PubMed:24992209,",
      "ECO:0000269|PubMed:27613864}."
    )
    val input = UniprotEntry(comments = commentsRaw)
    // when
    val results = updateComments(input)
    // then
    results.locations should have size 6
    results.functions should have size 1
  }

  "Database cross references" should "only include databases of interest" in {
    // given
    val inputs = Seq(
      "PIR; T00360; T00360.",
      "RefSeq; NP_001135906.1; NM_001142434.1. [O60502-4]",
      "RefSeq; NP_036347.1; NM_012215.3. [O60502-1]",
      "PDB; 2YDQ; X-ray; 2.60 A; T=402-408."
    )

    // when
    val results = inputs.filter(isDbOfInterest)
    // then
    results should have size 1

  }
  they should "only include the 'resource abbreviation' and 'resource identifier'" in {
    // given
    val inputs = Table(
      ("raw", "processed"),
      ("PDB; 2YDQ; X-ray; 2.60 A; T=402-408.", "PDB 2YDQ"),
      ("GO; GO:0005829; C:cytosol; IDA:UniProtKB.", "GO GO:0005829"),
      ("InterPro; IPR016181; Acyl_CoA_acyltransferase.", "InterPro IPR016181"),
      ("Pfam; PF07555; NAGidase; 1.", "Pfam PF07555"),
      ("DrugBank; DB12695; Phenethyl Isothiocyanate.", "DrugBank DB12695"),
      ("Ensembl; ENST00000296220; ENSP00000296220; ENSG00000144909.", "Ensembl ENST00000296220"),
      ("Reactome; R-HSA-162791; Attachment of GPI anchor to uPAR.", "Reactome R-HSA-162791")
    )
    forAll(inputs) { (in: String, out: String) =>
      {
        // when
        val results = extractIdentifiers(in)
        // then
        results should be(out)
      }
    }
  }

}
