package io.opentargets.etl.backend

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
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

  case class EvidenceInputsSection(
      rawEvidences: IOResourceConfig,
      diseases: IOResourceConfig,
      targets: IOResourceConfig,
      mechanismOfAction: IOResourceConfig
  )

  case class SucceedFailedOutputs(succeeded: IOResourceConfig, failed: IOResourceConfig)

  case class EvidencesSection(
      inputs: EvidenceInputsSection,
      uniqueFields: List[String],
      scoreExpr: String,
      datatypeId: String,
      dataSourcesExclude: List[String],
      dataSources: List[EvidenceEntry],
      directionOfEffect: DirectionOfEffectSection,
      outputs: SucceedFailedOutputs
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

  case class AOTFInputsSection(
      evidences: IOResourceConfig,
      diseases: IOResourceConfig,
      targets: IOResourceConfig
  )

  case class AOTFOutputsSection(clickhouse: IOResourceConfig)

  case class AOTFSection(
      outputs: AOTFOutputsSection,
      inputs: AOTFInputsSection
  )

  case class InputExtension(extensionType: String, input: IOResourceConfig)

  case class DrugOutputs(
      drug: IOResourceConfig,
      mechanismOfAction: IOResourceConfig,
      indications: IOResourceConfig,
      warnings: IOResourceConfig
  )
  case class DrugSection(
      chemicalProbes: IOResourceConfig,
      chemblMolecule: IOResourceConfig,
      chemblIndication: IOResourceConfig,
      chemblMechanism: IOResourceConfig,
      chemblTarget: IOResourceConfig,
      chemblWarning: IOResourceConfig,
      drugbankToChembl: IOResourceConfig,
      drugExtensions: Seq[InputExtension],
      diseaseEtl: IOResourceConfig,
      targetEtl: IOResourceConfig,
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
      interactionsEvidence: IOResourceConfig,
      interactionsUnmatched: IOResourceConfig
  )

  case class InteractionsSection(
      scorethreshold: Int,
      stringVersion: String,
      targetEtl: IOResourceConfig,
      rnacentral: IOResourceConfig,
      humanmapping: IOResourceConfig,
      ensproteins: IOResourceConfig,
      intact: IOResourceConfig,
      strings: IOResourceConfig,
      outputs: InteractionsOutput
  )

  case class ExpressionSection(
      rna: IOResourceConfig,
      binned: IOResourceConfig,
      zscore: IOResourceConfig,
      tissues: IOResourceConfig,
      efomap: IOResourceConfig,
      exprhierarchy: IOResourceConfig,
      output: IOResourceConfig
  )

  case class MetadataConfig(writeMode: String, output: IOResourceConfig)

  case class Common(
      input: String,
      output: String,
      error: String,
      outputFormat: String,
      additionalOutputs: List[String],
      metadata: MetadataConfig
  )

  case class KnownDrugsInputsSection(
      evidences: IOResourceConfig,
      diseases: IOResourceConfig,
      targets: IOResourceConfig,
      drugs: DrugOutputs
  )

  case class KnownDrugsSection(inputs: KnownDrugsInputsSection, output: IOResourceConfig)

  case class GeneOntologySection(goInput: IOResourceConfig, output: IOResourceConfig)

  case class TargetValidationInput(name: String, idColumn: String, data: IOResourceConfig)

  case class TargetValidation(
      inputs: Seq[TargetValidationInput],
      target: IOResourceConfig,
      output: SucceedFailedOutputs
  )

  case class SearchInputsSection(
      evidences: IOResourceConfig,
      diseases: IOResourceConfig,
      diseaseHpo: IOResourceConfig,
      hpo: IOResourceConfig,
      targets: IOResourceConfig,
      drugs: DrugOutputs,
      associations: IOResourceConfig
  )

  case class SearchOutputsSection(
      targets: IOResourceConfig,
      diseases: IOResourceConfig,
      drugs: IOResourceConfig
  )

  case class SearchSection(inputs: SearchInputsSection, outputs: SearchOutputsSection)

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

  case class PharmacogenomicsInputs(pgkb: IOResourceConfig, drug: IOResourceConfig)

  case class PharmacogenomicsSection(inputs: PharmacogenomicsInputs, outputs: IOResourceConfig)

  case class ReactomeSectionInputs(pathways: IOResourceConfig, relations: IOResourceConfig)

  case class ReactomeSection(inputs: ReactomeSectionInputs, output: IOResourceConfig)

  case class Target(input: TargetInput, outputs: TargetOutput, hgncOrthologSpecies: List[String])

  case class TargetInput(
      chemicalProbes: IOResourceConfig,
      hgnc: IOResourceConfig,
      ensembl: IOResourceConfig,
      uniprot: IOResourceConfig,
      uniprotSsl: IOResourceConfig,
      geneEssentiality: IOResourceConfig,
      genCode: IOResourceConfig,
      geneOntology: IOResourceConfig,
      geneOntologyRna: IOResourceConfig,
      geneOntologyRnaLookup: IOResourceConfig,
      geneOntologyEco: IOResourceConfig,
      tep: IOResourceConfig,
      hpa: IOResourceConfig,
      hpaSlOntology: IOResourceConfig,
      hallmarks: IOResourceConfig,
      ncbi: IOResourceConfig,
      psEssentialityMatrix: IOResourceConfig,
      psGeneIdentifier: IOResourceConfig,
      chembl: IOResourceConfig,
      geneticConstraints: IOResourceConfig,
      homologyDictionary: IOResourceConfig,
      homologyCodingProteins: IOResourceConfig,
      homologyGeneDictionary: IOResourceConfig,
      tractability: IOResourceConfig,
      safetyEvidence: IOResourceConfig,
      reactomeEtl: IOResourceConfig,
      reactomePathways: IOResourceConfig
  )

  case class TargetOutput(target: IOResourceConfig, geneEssentiality: IOResourceConfig)

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
      diseaseEtl: IOResourceConfig,
      output: IOResourceConfig,
      otarMeta: IOResourceConfig,
      otarProjectToEfo: IOResourceConfig
  )

  // --- EBISearch configuration
  case class EBISearchOutputSection(
      ebisearchAssociations: IOResourceConfig,
      ebisearchEvidence: IOResourceConfig
  )

  case class EBISearchSection(
      diseaseEtl: IOResourceConfig,
      targetEtl: IOResourceConfig,
      associationETL: IOResourceConfig,
      evidenceETL: IOResourceConfig,
      outputs: EBISearchOutputSection
  )

  // --- OpenFDA FAERS configuration --- //
  case class OpenfdaMontecarloSection(permutations: Int, percentile: Double)

  case class OpenfdaSamplingSection(size: Double, enabled: Boolean)

  case class OpenfdaOutputsSection(
      fdaUnfiltered: IOResourceConfig,
      fdaResults: IOResourceConfig,
      fdaTargetsUnfiltered: IOResourceConfig,
      fdaTargetsResults: IOResourceConfig,
      sampling: IOResourceConfig,
      samplingTargets: IOResourceConfig
  )
  case class OpenfdaMeddraSection(
      meddraPreferredTerms: IOResourceConfig,
      meddraLowLevelTerms: IOResourceConfig
  )

  case class OpenfdaSection(
      stepRootInputPath: String,
      stepRootOutputPath: String,
      chemblDrugs: IOResourceConfig,
      fdaData: IOResourceConfig,
      blacklistedEvents: IOResourceConfig,
      meddra: Option[OpenfdaMeddraSection],
      meddraPreferredTermsCols: List[String],
      meddraLowLevelTermsCols: List[String],
      montecarlo: OpenfdaMontecarloSection,
      sampling: OpenfdaSamplingSection,
      outputs: OpenfdaOutputsSection
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
      input: String,
      output: String,
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
      disease: DiseaseSection,
      interactions: InteractionsSection,
      knownDrugs: KnownDrugsSection,
      geneOntology: GeneOntologySection,
      search: SearchSection,
      aotf: AOTFSection,
      target: Target,
      targetValidation: TargetValidation,
      expression: ExpressionSection,
      openfda: OpenfdaSection,
      ebisearch: EBISearchSection,
      otarproject: OtarProjectSection,
      literature: LiteratureSection,
      targetEngine: TargetEngineSection
  )
}
