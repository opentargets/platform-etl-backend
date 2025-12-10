package io.opentargets.etl.backend

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.IoHelpers.IOResourceConfigurations
import io.opentargets.etl.backend.spark.{IOResourceConfig, IOResourceConfigOption}
import pureconfig.ConfigReader.Result
import pureconfig._
import pureconfig.generic.auto._
import pureconfig.generic.ProductHint

object Configuration extends LazyLogging {
  implicit def hint[A]: ProductHint[A] = ProductHint[A](ConfigFieldMapping(CamelCase, SnakeCase))

  lazy val config: Result[OTConfig] = load

  def load: ConfigReader.Result[OTConfig] = {
    val config = ConfigFactory.load()
    val obj = ConfigSource.fromConfig(config).load[OTConfig]
    logger.debug(s"configuration parsed successfully ${obj.toString}")

    obj
  }

  // step confuguration classes

  // association
  case class DataSource(
      id: String,
      weight: Double,
      dataType: String,
      propagate: Boolean
  )

  case class AssociationSection(
      output: IOResourceConfigurations,
      input: IOResourceConfigurations,
      defaultWeight: Double,
      defaultPropagate: Boolean,
      dataSources: List[DataSource]
  )

  // association_otf
  case class AssociationOTFSection(
      output: IOResourceConfigurations,
      input: IOResourceConfigurations
  )

  // drug
  case class InputExtension(
      extensionType: String,
      input: IOResourceConfig
  )

  case class DrugSection(
      input: IOResourceConfigurations,
      drugExtensions: Seq[InputExtension],
      output: IOResourceConfigurations
  )

  // expression
  case class ExpressionSection(
      input: IOResourceConfigurations,
      output: IOResourceConfigurations
  )

  // go
  case class GOSection(
      input: IOResourceConfigurations,
      output: IOResourceConfigurations
  )

  // interation
  case class InteractionSection(
      scorethreshold: Int,
      stringVersion: String,
      input: IOResourceConfigurations,
      output: IOResourceConfigurations
  )

  // known_drug
  case class KnownDrugSection(
      input: IOResourceConfigurations,
      output: IOResourceConfigurations
  )

  // literature
  case class LiteratureSectionRanks(
      section: String,
      rank: Long,
      weight: Double
  )

  case class LiteratureCommon(
      publicationSectionRanks: List[LiteratureSectionRanks],
      sparkSessionConfig: Option[Seq[IOResourceConfigOption]] = None
  )

  case class LiteratureProcessing(
      writeFailures: Boolean
  )

  case class LiteratureModelConfiguration(
      windowSize: Int,
      numPartitions: Int,
      maxIter: Int,
      minCount: Int,
      stepSize: Double
  )

  case class LiteratureEmbedding(
      modelConfiguration: LiteratureModelConfiguration
  )

  case class EpmcUris(
      ensembl: String,
      chembl: String,
      ontologies: String
  )

  case class Epmc(
      uris: EpmcUris,
      excludedTargetTerms: List[String],
      sectionsOfInterest: List[String],
      printMetrics: Boolean
  )

  case class LiteratureSection(
      common: LiteratureCommon,
      input: IOResourceConfigurations,
      output: IOResourceConfigurations,
      processing: LiteratureProcessing,
      embedding: LiteratureEmbedding,
      epmc: Epmc
  )

  // openfda
  case class OpenfdaMontecarloSection(
      permutations: Int,
      percentile: Double
  )

  case class OpenfdaSamplingSection(
      size: Double,
      enabled: Boolean
  )

  case class OpenfdaSection(
      input: IOResourceConfigurations,
      meddraPreferredTermsCols: List[String],
      meddraLowLevelTermsCols: List[String],
      montecarlo: OpenfdaMontecarloSection,
      sampling: OpenfdaSamplingSection,
      output: IOResourceConfigurations
  )

  // otar
  case class OtarSection(
      input: IOResourceConfigurations,
      output: IOResourceConfigurations
  )

  // pharmacogenomics
  case class PharmacogenomicsSection(
      input: IOResourceConfigurations,
      output: IOResourceConfigurations
  )

  // reactome
  case class ReactomeSection(
      input: IOResourceConfigurations,
      output: IOResourceConfigurations
  )

  // search
  case class SearchSection(
      input: IOResourceConfigurations,
      output: IOResourceConfigurations
  )

  // search_ebi
  case class SearchEbiSection(
      input: IOResourceConfigurations,
      output: IOResourceConfigurations
  )

  // search_facet
  case class FacetSearchCategories(
      diseaseName: String,
      therapeuticArea: String,
      SM: String,
      AB: String,
      PR: String,
      OC: String,
      targetId: String,
      approvedSymbol: String,
      approvedName: String,
      subcellularLocation: String,
      targetClass: String,
      pathways: String,
      goF: String,
      goP: String,
      goC: String
  )

  case class SearchFacetSection(
      input: IOResourceConfigurations,
      output: IOResourceConfigurations,
      categories: FacetSearchCategories
  )

  // target
  case class TargetSection(
      input: IOResourceConfigurations,
      output: IOResourceConfigurations,
      hgncOrthologSpecies: List[String]
  )

  // target_engine
  case class TargetEngineSection(
      input: IOResourceConfigurations,
      output: IOResourceConfigurations
  )

  case class Steps(
      association: AssociationSection,
      associationOtf: AssociationOTFSection,
      drug: DrugSection,
      expression: ExpressionSection,
      go: GOSection,
      interaction: InteractionSection,
      knownDrug: KnownDrugSection,
      literature: LiteratureSection,
      openfda: OpenfdaSection,
      otar: OtarSection,
      pharmacogenomics: PharmacogenomicsSection,
      reactome: ReactomeSection,
      search: SearchSection,
      searchEbi: SearchEbiSection,
      searchFacet: SearchFacetSection,
      target: TargetSection,
      targetEngine: TargetEngineSection
  )

  // main config classes
  case class SparkSettings(
      writeMode: String,
      defaultSparkSessionConfig: Seq[IOResourceConfigOption]
  ) {
    val validWriteModes = Set("error", "errorifexists", "append", "overwrite", "ignore")
    require(
      validWriteModes.contains(writeMode),
      s"$writeMode is not valid. Must be one of ${validWriteModes.toString()}"
    )
  }

  case class Common(
      path: String,
      outputFormat: String,
      additionalOutputs: List[String]
  )

  case class OTConfig(
      sparkUri: Option[String],
      sparkSettings: SparkSettings,
      steps: Steps,
      common: Common
  )
}
