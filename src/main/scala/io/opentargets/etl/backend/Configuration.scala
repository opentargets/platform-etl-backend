package io.opentargets.etl.backend

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.IoHelpers.IOResourceConfigurations
import io.opentargets.etl.backend.spark.{IOResourceConfig, IOResourceConfigOption}
import pureconfig.ConfigReader.Result
import pureconfig._
import pureconfig.generic.auto._

object Configuration extends LazyLogging {
  lazy val config: Result[OTConfig] = load

  def load: ConfigReader.Result[OTConfig] = {
    logger.info("load configuration from file")
    val config = ConfigFactory.load()

    val obj = ConfigSource.fromConfig(config).load[OTConfig]
    logger.debug(s"configuration properly case classed ${obj.toString}")

    obj
  }

  case class DataSource(id: String, weight: Double, dataType: String, propagate: Boolean)

  case class EpmcUris(ensembl: String, chembl: String, ontologies: String)

  case class Epmc(
      uris: EpmcUris,
      excludedTargetTerms: List[String],
      sectionsOfInterest: List[String],
      printMetrics: Boolean
  )

  case class EvidenceEntry(
      id: String,
      uniqueFields: List[String],
      datatypeId: Option[String],
      scoreExpr: String,
      excludedBiotypes: Option[List[String]]
  )

  case class EvidencesSection(
      input: IOResourceConfigurations,
      uniqueFields: List[String],
      scoreExpr: String,
      datatypeId: String,
      dataSourcesExclude: List[String],
      dataSources: List[EvidenceEntry],
      directionOfEffect: DirectionOfEffectSection,
      output: IOResourceConfigurations
  )

  case class DirectionOfEffectSection(
      varFilterLof: List[String],
      gof: List[String],
      lof: List[String],
      oncotsgList: List[String],
      inhibitors: List[String],
      activators: List[String],
      sources: List[String]
  )

  case class AssociationsSection(
      output: IOResourceConfigurations,
      input: IOResourceConfigurations,
      defaultWeight: Double,
      defaultPropagate: Boolean,
      dataSources: List[DataSource]
  )

  case class AOTFSection(
      output: IOResourceConfigurations,
      input: IOResourceConfigurations
  )

  case class InputExtension(extensionType: String, input: IOResourceConfig)

  case class DrugSection(
      input: IOResourceConfigurations,
      drugExtensions: Seq[InputExtension],
      output: IOResourceConfigurations
  )

  case class InteractionsSection(
      scorethreshold: Int,
      stringVersion: String,
      input: IOResourceConfigurations,
      output: IOResourceConfigurations
  )

  case class ExpressionSection(input: IOResourceConfigurations, output: IOResourceConfigurations)

  case class Common(
      path: String,
      outputFormat: String,
      additionalOutputs: List[String]
  )

  case class KnownDrugsSection(input: IOResourceConfigurations, output: IOResourceConfigurations)

  case class GeneOntologySection(input: IOResourceConfigurations, output: IOResourceConfigurations)

  case class MousePhenotypeSection(input: IOResourceConfigurations, output: IOResourceConfigurations)

  case class SearchSection(input: IOResourceConfigurations, output: IOResourceConfigurations)

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

  case class FacetSearchSection(input: IOResourceConfigurations,
                                output: IOResourceConfigurations,
                                categories: FacetSearchCategories
  )

  case class PharmacogenomicsSection(input: IOResourceConfigurations, output: IOResourceConfigurations)

  case class ReactomeSection(input: IOResourceConfigurations, output: IOResourceConfigurations)

  case class Target(input: IOResourceConfigurations, output: IOResourceConfigurations, hgncOrthologSpecies: List[String])

  case class SparkSettings(writeMode: String,
                           ignoreIfExists: Boolean,
                           defaultSparkSessionConfig: Seq[IOResourceConfigOption]
  ) {
    val validWriteModes = Set("error", "errorifexists", "append", "overwrite", "ignore")
    require(
      validWriteModes.contains(writeMode),
      s"$writeMode is not valid. Must be one of ${validWriteModes.toString()}"
    )
  }

  // --- OtarProject configuration
  case class OtarProjectSection(
      input: IOResourceConfigurations,
      output: IOResourceConfigurations
  )

  case class EBISearchSection(
      input: IOResourceConfigurations,
      output: IOResourceConfigurations
  )

  // --- OpenFDA FAERS configuration --- //
  case class OpenfdaMontecarloSection(permutations: Int, percentile: Double)

  case class OpenfdaSamplingSection(size: Double, enabled: Boolean)

  case class OpenfdaSection(
      input: IOResourceConfigurations,
      meddraPreferredTermsCols: List[String],
      meddraLowLevelTermsCols: List[String],
      montecarlo: OpenfdaMontecarloSection,
      sampling: OpenfdaSamplingSection,
      output: IOResourceConfigurations
  )

  case class LiteratureProcessing(writeFailures: Boolean)

  case class LiteratureModelConfiguration(windowSize: Int,
                                          numPartitions: Int,
                                          maxIter: Int,
                                          minCount: Int,
                                          stepSize: Double
  )

  case class LiteratureEmbedding(modelConfiguration: LiteratureModelConfiguration)

  case class LiteratureSectionRanks(section: String, rank: Long, weight: Double)

  case class LiteratureCommon(publicationSectionRanks: List[LiteratureSectionRanks],
                              sparkSessionConfig: Option[Seq[IOResourceConfigOption]] = None
  )

  case class LiteratureSection(
      common: LiteratureCommon,
      input: IOResourceConfigurations,
      output: IOResourceConfigurations,
      processing: LiteratureProcessing,
      embedding: LiteratureEmbedding,
      epmc: Epmc
  )

  case class TargetEngineSection(input: IOResourceConfigurations, output: IOResourceConfigurations)

  // --- END --- //

  case class EtlStep[T](step: T, dependencies: List[T])

  case class OTConfig(
      sparkUri: Option[String],
      sparkSettings: SparkSettings,
      steps: List[String],
      common: Common,
      pharmacogenomics: PharmacogenomicsSection,
      reactome: ReactomeSection,
      association: AssociationsSection,
      evidence: EvidencesSection,
      searchFacet: FacetSearchSection,
      drug: DrugSection,
      interaction: InteractionsSection,
      knownDrug: KnownDrugsSection,
      go: GeneOntologySection,
      search: SearchSection,
      associationOtf: AOTFSection,
      target: Target, // TODO: rename to match the rest of the sections
      mousePhenotype: MousePhenotypeSection,
      expression: ExpressionSection,
      openfda: OpenfdaSection,
      searchEbi: EBISearchSection,
      otar: OtarProjectSection,
      literature: LiteratureSection,
      targetEngine: TargetEngineSection
  )
}
