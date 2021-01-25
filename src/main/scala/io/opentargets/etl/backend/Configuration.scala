package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import com.typesafe.config.ConfigFactory
import io.opentargets.etl.backend.spark.Helpers.IOResourceConfig
import pureconfig.ConfigReader.Result
import pureconfig._
import pureconfig.generic.auto._

object Configuration extends LazyLogging {
  lazy val config: Result[OTConfig] = load

  case class DataSource(id: String, weight: Double, dataType: String, propagate: Boolean)

  case class EvidenceEntry(id: String, uniqueFields: List[String], scoreExpr: String)

  case class EvidenceInputsSection(rawEvidences: IOResourceConfig,
                                   diseases: IOResourceConfig,
                                   targets: IOResourceConfig)

  case class EvidenceOutputsSection(succeeded: IOResourceConfig,
                                    failed: IOResourceConfig,
                                    stats: IOResourceConfig)

  case class EvidencesSection(inputs: EvidenceInputsSection,
                              uniqueFields: List[String],
                              scoreExpr: String,
                              dataSources: List[EvidenceEntry],
                              outputs: EvidenceOutputsSection)

  case class AssociationInputsSection(evidences: IOResourceConfig,
                                      diseases: IOResourceConfig,
                                      targets: IOResourceConfig)

  case class AssociationOutputsSection(directByDatasource: IOResourceConfig,
                                       directByOverall: IOResourceConfig,
                                       indirectByDatasource: IOResourceConfig,
                                       indirectByOverall: IOResourceConfig)

  case class AssociationsSection(
      outputs: AssociationOutputsSection,
      inputs: AssociationInputsSection,
      defaultWeight: Double,
      defaultPropagate: Boolean,
      dataSources: List[DataSource]
  )

  case class AOTFInputsSection(evidences: IOResourceConfig,
                               diseases: IOResourceConfig,
                               targets: IOResourceConfig)

  case class AOTFOutputsSection(clickhouse: IOResourceConfig,
                                elasticsearch: IOResourceConfig)

  case class AOTFSection(
      outputs: AOTFOutputsSection,
      inputs: AOTFInputsSection
  )

  case class EvidenceProteinFix(input: String, output: String)

  case class InteractionsSection(
      rnacentral: InputInfo,
      humanmapping: InputInfo,
      ensproteins: InputInfo,
      intact: InputInfo,
      strings: InputInfo
  )

  case class InputInfo(format: String, path: String)

  case class InputExtension(extensionType: String, input: IOResourceConfig)

  case class DrugOutputs(drug: IOResourceConfig, mechanismOfAction: IOResourceConfig, indications: IOResourceConfig)
  case class DrugSection(
                          chemblMolecule: IOResourceConfig,
                          chemblIndication: IOResourceConfig,
                          chemblMechanism: IOResourceConfig,
                          chemblTarget: IOResourceConfig,
                          drugbankToChembl: IOResourceConfig,
                          drugExtensions: Seq[InputExtension],
                          diseaseEtl: IOResourceConfig,
                          targetEtl: IOResourceConfig,
                          evidenceEtl: IOResourceConfig,
                          outputs: DrugOutputs
  )

  case class HpoOutputs(hpo: IOResourceConfig, diseaseHpo: IOResourceConfig)
  case class HpoSection(
                        diseaseEtl: IOResourceConfig,
                        mondoOntology: IOResourceConfig,
                        hpoOntology: IOResourceConfig,
                        hpoPhenotype: IOResourceConfig,
                        outputs: HpoOutputs
  )

  case class DiseaseOutput(diseases: IOResourceConfig)
  case class DiseaseSection(
                            efoOntology: IOResourceConfig,
                            outputs: DiseaseOutput
  )

  case class Inputs(
      target: InputInfo,
      evidence: InputInfo,
      ddr: InputInfo,
      reactome: InputInfo,
      eco: InputInfo,
      expression: InputInfo,
      tep: InputInfo,
      mousephenotypes: InputInfo,
      interactions: InteractionsSection
  )

  case class Common(defaultSteps: Seq[String], inputs: Inputs, output: String, outputFormat: String)

  case class KnownDrugsInputsSection(evidences: IOResourceConfig,
                                     diseases: IOResourceConfig,
                                     targets: IOResourceConfig,
                                     drugs: DrugOutputs)

  case class KnownDrugsSection(inputs: KnownDrugsInputsSection, output: IOResourceConfig)

  case class SearchInputsSection(evidences: IOResourceConfig,
                                 diseases: IOResourceConfig,
                                 targets: IOResourceConfig,
                                 drugs: DrugOutputs,
                                 associations: IOResourceConfig)

  case class SearchOutputsSection(targets: IOResourceConfig,
                                 diseases: IOResourceConfig,
                                 drugs: IOResourceConfig)

  case class SearchSection(inputs: SearchInputsSection, outputs: SearchOutputsSection)

  case class OTConfig(
      sparkUri: Option[String],
      common: Common,
      evidenceProteinFix: EvidenceProteinFix,
      associations: AssociationsSection,
      evidences: EvidencesSection,
      drug: DrugSection,
      disease: DiseaseSection,
      hpo: HpoSection,
      knownDrugs: KnownDrugsSection,
      search: SearchSection,
      aotf: AOTFSection
  )

  def load: ConfigReader.Result[OTConfig] = {
    logger.info("load configuration from file")
    val config = ConfigFactory.load()

    val obj = ConfigSource.fromConfig(config).load[OTConfig]
    logger.debug(s"configuration properly case classed ${obj.toString}")

    obj
  }
}
