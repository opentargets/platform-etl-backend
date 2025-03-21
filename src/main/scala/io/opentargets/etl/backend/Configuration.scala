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

  case class EpmcInputs(cooccurences: IOResourceConfig)

  case class EpmcOutputs(
      output: IOResourceConfig,
      epmcCooccurrences: IOResourceConfig
  )

  case class EpmcUris(ensembl: String, chembl: String, ontologies: String)

  case class Epmc(
      uris: EpmcUris,
      input: EpmcInputs,
      outputs: EpmcOutputs,
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

  case class AssociationInputsSection(evidences: IOResourceConfig, diseases: IOResourceConfig)

  case class AssociationOutputsSection(
      directByDatasource: IOResourceConfig,
      directByDatatype: IOResourceConfig,
      directByOverall: IOResourceConfig,
      indirectByDatasource: IOResourceConfig,
      indirectByDatatype: IOResourceConfig,
      indirectByOverall: IOResourceConfig
  )

  case class AssociationsSection(
      outputs: AssociationOutputsSection,
      inputs: AssociationInputsSection,
      defaultWeight: Double,
      defaultPropagate: Boolean,
      dataSources: List[DataSource]
  )

  case class AOTFSection(
      output: IOResourceConfigurations,
      input: IOResourceConfigurations
  )

  case class InputExtension(extensionType: String, input: IOResourceConfig)

  case class DrugOutputs(
      drug: IOResourceConfig,
      mechanismOfAction: IOResourceConfig,
      indications: IOResourceConfig,
      warnings: IOResourceConfig
  )
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

  case class GeneOntologySection(goInput: IOResourceConfig, output: IOResourceConfig)

  case class MousePhenotypeSection(input: IOResourceConfigurations,
                                   output: IOResourceConfigurations
  )

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

  case class FacetSearchInputsSection(
      diseases: IOResourceConfig,
      targets: IOResourceConfig,
      go: IOResourceConfig
  )
  case class FacetSearchOutputsSection(
      diseases: IOResourceConfig,
      targets: IOResourceConfig
  )
  case class FacetSearchSection(inputs: FacetSearchInputsSection,
                                outputs: FacetSearchOutputsSection,
                                categories: FacetSearchCategories
  )

  case class PharmacogenomicsSection(input: IOResourceConfigurations,
                                     output: IOResourceConfigurations
  )

  case class ReactomeSection(input: IOResourceConfigurations, output: IOResourceConfigurations)

  case class Target(input: IOResourceConfigurations,
                    output: IOResourceConfigurations,
                    hgncOrthologSpecies: List[String]
  )

  case class SparkSettings(writeMode: String,
                           ignoreIfExists: Boolean,
                           defaultSparkSessionConfig: Seq[IOResourceConfigOption]
  ) {
    val validWriteModes = Set("error", "errorifexists", "append", "overwrite") // TODO: add ignore
    require(
      validWriteModes.contains(writeMode),
      s"$writeMode is not valid. Must be one of ${validWriteModes.toString()}"
    )
  }

  // --- OtarProject configuration
  case class OtarProjectSection(
      diseaseEtl: IOResourceConfig,
      output: IOResourceConfig,
      otarMeta: IOResourceConfig,
      otarProjectToEfo: IOResourceConfig
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

  case class LiteratureProcessingOutputs(cooccurrences: IOResourceConfig,
                                         matches: IOResourceConfig,
                                         failedCooccurrences: IOResourceConfig,
                                         failedMatches: IOResourceConfig,
                                         literatureIndex: IOResourceConfig,
                                         literatureSentences: IOResourceConfig
  )

  case class LiteratureProcessing(epmcids: IOResourceConfig,
                                  diseases: IOResourceConfig,
                                  targets: IOResourceConfig,
                                  drugs: IOResourceConfig,
                                  abstracts: EPMCInput,
                                  fullTexts: EPMCInput,
                                  outputs: LiteratureProcessingOutputs,
                                  writeFailures: Boolean
  )

  case class EPMCInput(kind: String, input: IOResourceConfig)

  case class LiteratureModelConfiguration(windowSize: Int,
                                          numPartitions: Int,
                                          maxIter: Int,
                                          minCount: Int,
                                          stepSize: Double
  )

  case class LiteratureEmbeddingOutputs(model: IOResourceConfig, trainingSet: IOResourceConfig)

  case class LiteratureEmbedding(modelConfiguration: LiteratureModelConfiguration,
                                 input: IOResourceConfig,
                                 outputs: LiteratureEmbeddingOutputs
  )

  case class LiteratureVectors(input: String, output: IOResourceConfig)

  case class LiteratureSectionRanks(section: String, rank: Long, weight: Double)

  case class LiteratureCommon(publicationSectionRanks: List[LiteratureSectionRanks],
                              sparkSessionConfig: Option[Seq[IOResourceConfigOption]] = None
  )

  case class LiteratureSection(
      common: LiteratureCommon,
      processing: LiteratureProcessing,
      embedding: LiteratureEmbedding,
      vectors: LiteratureVectors,
      epmc: Epmc
  )

  case class TargetEngineInputs(targets: IOResourceConfig,
                                molecule: IOResourceConfig,
                                mechanismOfAction: IOResourceConfig,
                                mousePhenotypes: IOResourceConfig,
                                hpaData: IOResourceConfig,
                                uniprotSlterms: IOResourceConfig,
                                mousePhenoScores: IOResourceConfig
  )

  case class TargetEngineOutputs(targetEngine: IOResourceConfig)

  case class TargetEngineSection(inputs: TargetEngineInputs, outputs: TargetEngineOutputs)

  // --- END --- //

  case class EtlStep[T](step: T, dependencies: List[T])

  case class EtlDagConfig(steps: List[EtlStep[String]], resolve: Boolean)

  case class OTConfig(
      sparkUri: Option[String],
      sparkSettings: SparkSettings,
      etlDag: EtlDagConfig,
      common: Common,
      pharmacogenomics: PharmacogenomicsSection,
      reactome: ReactomeSection,
      associations: AssociationsSection,
      evidences: EvidencesSection,
      facetSearch: FacetSearchSection,
      drug: DrugSection,
      interactions: InteractionsSection,
      knownDrugs: KnownDrugsSection,
      geneOntology: GeneOntologySection,
      search: SearchSection,
      aotf: AOTFSection,
      target: Target,//TODO: rename to match the rest of the sections
      mousePhenotype: MousePhenotypeSection,
      expression: ExpressionSection,
      openfda: OpenfdaSection,
      ebisearch: EBISearchSection,
      otarproject: OtarProjectSection,
      literature: LiteratureSection,
      targetEngine: TargetEngineSection
  )
}
