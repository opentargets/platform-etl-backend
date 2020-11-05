package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import com.typesafe.config.ConfigFactory
import pureconfig.ConfigReader.Result
import pureconfig._
import pureconfig.generic.auto._

object Configuration extends LazyLogging {
  lazy val config: Result[OTConfig] = load

  case class DataSource(id: String, weight: Double, dataType: String, propagate: Boolean)

  case class EvidenceEntry(id: String, uniqueFields: List[String], scoreSQL: String)

  case class EvidencesSection(input: InputInfo,
                              requiredFields: List[String],
                              datasources: List[EvidenceEntry])

  case class AssociationsSection(
      defaultWeight: Double,
      defaultPropagate: Boolean,
      dataSources: List[DataSource]
  )

  case class ClinicalTrials(
      studies: String,
      studyReferences: String,
      countries: String,
      sponsors: String,
      interventions: String,
      interventionsOtherNames: String,
      interventionsMesh: String,
      conditions: String,
      conditionsMesh: String
  )

  case class Dailymed(rxnormMapping: String, prescriptionData: String)

  case class EvidenceProteinFix(input: String, output: String)

  case class InteractionsSection(
      rnacentral: InputInfo,
      humanmapping: InputInfo,
      ensproteins: InputInfo,
      intact: InputInfo,
      strings: InputInfo
  )

  case class InputInfo(format: String, path: String)

  case class InputExtension(extensionType: String, path: String) {
    require(path.endsWith("json"))
  }

  case class Inputs(
      target: InputInfo,
      disease: InputInfo,
      drug: InputInfo,
      drugChemblMolecule: InputInfo,
      drugChemblIndication: InputInfo,
      drugChemblMechanism: InputInfo,
      drugChemblTarget: InputInfo,
      drugDrugbankToChembl: InputInfo,
      drugExtensions: Seq[InputExtension],
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

  case class OTConfig(
      sparkUri: Option[String],
      common: Common,
      clinicalTrials: ClinicalTrials,
      dailymed: Dailymed,
      evidenceProteinFix: EvidenceProteinFix,
      associations: AssociationsSection,
      evidences: EvidencesSection
  )

  def load: ConfigReader.Result[OTConfig] = {
    logger.info("load configuration from file")
    val config = ConfigFactory.load()

    val obj = ConfigSource.fromConfig(config).load[OTConfig]
    logger.debug(s"configuration properly case classed ${obj.toString}")

    obj
  }
}
