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

  case class EvidenceEntry(id: String,
                           uniqueFields: List[String],
                           datatypeId: Option[String],
                           scoreExpr: String,
                           excludedBiotypes: Option[List[String]])

  case class EvidenceInputsSection(rawEvidences: IOResourceConfig,
                                   diseases: IOResourceConfig,
                                   targets: IOResourceConfig)

  case class EvidenceOutputsSection(succeeded: IOResourceConfig, failed: IOResourceConfig)

  case class EvidencesSection(inputs: EvidenceInputsSection,
                              uniqueFields: List[String],
                              scoreExpr: String,
                              datatypeId: String,
                              dataSources: List[EvidenceEntry],
                              outputs: EvidenceOutputsSection)

  case class AssociationInputsSection(evidences: IOResourceConfig,
                                      diseases: IOResourceConfig,
                                      targets: IOResourceConfig)

  case class AssociationOutputsSection(directByDatasource: IOResourceConfig,
                                       directByDatatype: IOResourceConfig,
                                       directByOverall: IOResourceConfig,
                                       indirectByDatasource: IOResourceConfig,
                                       indirectByDatatype: IOResourceConfig,
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

  case class AOTFOutputsSection(clickhouse: IOResourceConfig, elasticsearch: IOResourceConfig)

  case class AOTFSection(
      outputs: AOTFOutputsSection,
      inputs: AOTFInputsSection
  )

  case class InputInfo(format: String, path: String)

  case class InputExtension(extensionType: String, input: IOResourceConfig)

  case class DrugOutputs(drug: IOResourceConfig,
                         mechanismOfAction: IOResourceConfig,
                         indications: IOResourceConfig,
                         warnings: IOResourceConfig)
  case class DrugSection(
      chemblMolecule: IOResourceConfig,
      chemblIndication: IOResourceConfig,
      chemblMechanism: IOResourceConfig,
      chemblTarget: IOResourceConfig,
      chemblWarning: IOResourceConfig,
      drugbankToChembl: IOResourceConfig,
      drugExtensions: Seq[InputExtension],
      diseaseEtl: IOResourceConfig,
      targetEtl: IOResourceConfig,
      evidenceEtl: IOResourceConfig,
      outputs: DrugOutputs
  )

  case class DiseaseOutput(
      diseases: IOResourceConfig,
      hpo: IOResourceConfig,
      diseaseHpo: IOResourceConfig
  )
  case class DiseaseSection(
      efoOntology: IOResourceConfig,
      mondoOntology: IOResourceConfig,
      hpoOntology: IOResourceConfig,
      hpoPhenotype: IOResourceConfig,
      outputs: DiseaseOutput
  )

  case class InteractionsOutput(
      interactions: IOResourceConfig,
      interactionsEvidence: IOResourceConfig
  )

  case class InteractionsSection(
      scorethreshold: Int,
      targetEtl: IOResourceConfig,
      rnacentral: IOResourceConfig,
      humanmapping: IOResourceConfig,
      ensproteins: IOResourceConfig,
      intact: IOResourceConfig,
      strings: IOResourceConfig,
      outputs: InteractionsOutput
  )

  case class Inputs(
      target: InputInfo,
      reactome: InputInfo,
      eco: InputInfo,
      expression: InputInfo,
      tep: InputInfo,
      mousephenotypes: InputInfo
  )

  case class Common(defaultSteps: Seq[String],
                    input: String,
                    inputs: Inputs,
                    output: String,
                    outputFormat: String,
                    metadata: IOResourceConfig)

  case class KnownDrugsInputsSection(evidences: IOResourceConfig,
                                     diseases: IOResourceConfig,
                                     targets: IOResourceConfig,
                                     drugs: DrugOutputs)

  case class KnownDrugsSection(inputs: KnownDrugsInputsSection, output: IOResourceConfig)

  case class SearchInputsSection(evidences: IOResourceConfig,
                                 diseases: IOResourceConfig,
                                 diseaseHpo: IOResourceConfig,
                                 hpo: IOResourceConfig,
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
      associations: AssociationsSection,
      evidences: EvidencesSection,
      drug: DrugSection,
      disease: DiseaseSection,
      interactions: InteractionsSection,
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
