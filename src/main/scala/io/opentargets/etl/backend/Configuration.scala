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
  case class DrugSection(
      chemblMolecule: IOResourceConfig,
      chemblIndication: IOResourceConfig,
      chemblMechanism: IOResourceConfig,
      chemblTarget: IOResourceConfig,
      drugbankToChembl: IOResourceConfig,
      drugExtensions: Seq[InputExtension],
      diseasePipeline: IOResourceConfig,
      targetPipeline: IOResourceConfig,
      evidencePipeline: IOResourceConfig,
      output: IOResourceConfig
  )

  case class Inputs(
      target: InputInfo,
      disease: InputInfo,
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
                                     drugs: IOResourceConfig)

  case class KnownDrugsSection(inputs: KnownDrugsInputsSection, output: IOResourceConfig)

  case class SearchInputsSection(evidences: IOResourceConfig,
                                 diseases: IOResourceConfig,
                                 targets: IOResourceConfig,
                                 drugs: IOResourceConfig,
                                 associations: IOResourceConfig)

  case class SearchSection(inputs: SearchInputsSection, output: IOResourceConfig)

  case class OTConfig(
      sparkUri: Option[String],
      common: Common,
      evidenceProteinFix: EvidenceProteinFix,
      associations: AssociationsSection,
      evidences: EvidencesSection,
      drug: DrugSection,
      knownDrugs: KnownDrugsSection,
      search: SearchSection
  )

  def load: ConfigReader.Result[OTConfig] = {
    logger.info("load configuration from file")
    val config = ConfigFactory.load()

    val obj = ConfigSource.fromConfig(config).load[OTConfig]
    logger.debug(s"configuration properly case classed ${obj.toString}")

    obj
  }
}
