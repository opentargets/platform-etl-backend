package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers.{mkFlattenArray, nest, safeArrayUnion}
import io.opentargets.etl.backend.{Configuration, ETLSessionContext}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import io.opentargets.etl.backend.spark.{CsvHelpers, IOResource, IOResourceConfig, IoHelpers}
import io.opentargets.etl.preprocess.uniprot.UniprotConverter
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{
  array,
  array_contains,
  array_distinct,
  array_join,
  array_union,
  broadcast,
  coalesce,
  col,
  collect_list,
  collect_set,
  explode,
  expr,
  filter,
  flatten,
  lit,
  size,
  split,
  struct,
  trim,
  typedLit,
  udf,
  when
}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.jdk.CollectionConverters.asScalaIteratorConverter

object Target extends LazyLogging {
  def apply()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession

    val targetDF = compute(context)

    val dataframesToSave: IOResources = Map(
      "target" -> IOResource(targetDF, context.configuration.target.outputs.target)
    )

    IoHelpers.writeTo(dataframesToSave)

  }

  def compute(context: ETLSessionContext)(implicit ss: SparkSession): DataFrame = {

    // 1. get input data frames
    val inputDataFrames = getMappedInputs(context.configuration.target)

    // 2. prepare intermediate dataframes per source
    val chemicalProbes: DataFrame = inputDataFrames("chemicalProbes").data
    val hgnc: Dataset[Hgnc] = Hgnc(inputDataFrames("hgnc").data)
    val hallmarks: Dataset[HallmarksWithId] = Hallmarks(inputDataFrames("hallmarks").data)
    val ncbi: Dataset[Ncbi] = Ncbi(inputDataFrames("ncbi").data)
    val ensemblDf: Dataset[Ensembl] = Ensembl(inputDataFrames("ensembl").data)
    val uniprotDS: Dataset[Uniprot] =
      Uniprot(inputDataFrames("uniprot").data, inputDataFrames("uniprotSsl").data)
    val geneOntologyDf: Dataset[GeneOntologyByEnsembl] = GeneOntology(
      inputDataFrames("geneOntologyHuman").data,
      inputDataFrames("geneOntologyRna").data,
      inputDataFrames("geneOntologyRnaLookup").data,
      inputDataFrames("geneOntologyEcoLookup").data,
      ensemblDf
    )
    val tep: Dataset[Tep] = Tep(inputDataFrames("tep").data)
    val hpa: Dataset[GeneWithLocation] =
      GeneWithLocation(inputDataFrames("hpa").data, inputDataFrames("hpaSL").data)
    val projectScoresDS: Dataset[GeneWithDbXRef] = ProjectScores(
      inputDataFrames("projectScoresIds").data,
      inputDataFrames("projectScoresEssentialityMatrix").data
    )
    val proteinClassification: Dataset[ProteinClassification] = ProteinClassification(
      inputDataFrames("chembl").data
    )
    val geneticConstraints: Dataset[GeneticConstraintsWithId] = GeneticConstraints(
      inputDataFrames("geneticConstraints").data
    )
    val homology: Dataset[Ortholog] = Ortholog(
      inputDataFrames("homologyDictionary").data,
      inputDataFrames("homologyCodingProteins").data,
      inputDataFrames("homologyGeneDictionary").data,
      context.configuration.target.hgncOrthologSpecies
    )
    val reactome: Dataset[Reactomes] =
      Reactome(inputDataFrames("reactomePathways").data, inputDataFrames("reactomeEtl").data)
    val tractability: Dataset[TractabilityWithId] = Tractability(
      inputDataFrames("tractability").data
    )

    // 3. merge intermediate data frames into final
    val hgncEnsemblGoDF = mergeHgncAndEnsembl(hgnc, ensemblDf)
      .join(geneOntologyDf, ensemblDf("id") === geneOntologyDf("ensemblId"), "left_outer")
      .drop("ensemblId")
      .join(projectScoresDS, Seq("id"), "left_outer")
      .join(hallmarks, Seq("approvedSymbol"), "left_outer")

    val uniprotGroupedByEnsemblIdDF =
      addEnsemblIdsToUniprot(
        hgnc,
        addProteinClassificationToUniprot(uniprotDS, proteinClassification)
      )
        .withColumnRenamed("proteinIds", "pid")
        .join(hpa, Seq("id"), "left_outer")
        .withColumn(
          "subcellularLocations",
          mkFlattenArray(col("subcellularLocations"), col("locations"))
        )
        .drop("locations")

    val targetInterim = hgncEnsemblGoDF
      .join(uniprotGroupedByEnsemblIdDF, Seq("id"), "left_outer")
      .withColumn("proteinIds", safeArrayUnion(col("proteinIds"), col("pid")))
      .withColumn(
        "dbXrefs",
        safeArrayUnion(col("hgncId"), col("dbXrefs"), col("signalP"), col("xRef"))
      )
      .withColumn(
        "symbolSynonyms",
        safeArrayUnion(col("symbolSynonyms"), col("hgncSymbolSynonyms"))
      )
      .withColumn("nameSynonyms", safeArrayUnion(col("nameSynonyms"), col("hgncNameSynonyms")))
      .withColumn(
        "synonyms",
        safeArrayUnion(col("synonyms"), col("symbolSynonyms"), col("nameSynonyms"))
      )
      .withColumn("obsoleteSymbols", safeArrayUnion(col("hgncObsoleteSymbols")))
      .withColumn("obsoleteNames", safeArrayUnion(col("hgncObsoleteNames")))
      .drop(
        "pid",
        "hgncId",
        "hgncSynonyms",
        "hgncNameSynonyms",
        "hgncSymbolSynonyms",
        "hgncObsoleteNames",
        "hgncObsoleteSymbols",
        "uniprotIds",
        "signalP",
        "xRef"
      )
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val ensemblIdLookupDf = generateEnsgToSymbolLookup(targetInterim)

    targetInterim
      .join(geneticConstraints, Seq("id"), "left_outer")
      .transform(addTep(tep.toDF, ensemblIdLookupDf))
      .transform(filterAndSortProteinIds)
      .transform(removeRedundantXrefs)
      .transform(addChemicalProbes(chemicalProbes, ensemblIdLookupDf))
      .transform(addOrthologue(homology))
      .transform(addTractability(tractability))
      .transform(addNcbiEntrezSynonyms(ncbi))
      .transform(addTargetSafety(inputDataFrames, ensemblIdLookupDf))
      .transform(addReactome(reactome))
      .transform(removeDuplicatedSynonyms)
  }

  /** for all alternative names or symbols a target can take is worth cleaning them up from duplicated entries */
  private def removeDuplicatedSynonyms(df: DataFrame): DataFrame =
    ("synonyms" :: "symbolSynonyms" :: "nameSynonyms" :: "obsoleteNames" :: "obsoleteSymbols" :: Nil)
      .foldLeft(df) { (B, name) =>
        B.withColumn(name, array_distinct(col(name)))
      }

  /** Some of the input data sets do not use Ensembl Ids to identify records. Commonly we see uniprot
    * accessions or protein Ids. This dataframe can be used as a 'helper' to link these datasets with
    * the ENSG ID used in Open Targets.
    *
    * @param df interim target DF.
    * @return dataframe [ensgId, name, uniprot, HGNC] mapping ensembl Ids to other common sources.
    */
  private def generateEnsgToSymbolLookup(df: DataFrame): DataFrame = {
    df.select(
      col("id"),
      col("proteinIds.id") as "pid",
      array(col("approvedSymbol")) as "as",
      filter(col("synonyms"), _.getField("source") === "uniprot") as "uniprot",
      filter(col("synonyms"), _.getField("source") === "HGNC") as "HGNC",
      array_distinct(
        safeArrayUnion(
          col("proteinIds.id"),
          col("symbolSynonyms.label"),
          col("obsoleteSymbols.label"),
          array(col("approvedSymbol"))
        )
      ) as "symbols"
    ).select(
      col("id"),
      flatten(array(col("pid"), col("as"))) as "s",
      col("uniprot.label") as "uniprot",
      col("HGNC.label") as "HGNC",
      col("symbols")
    ).select(col("id") as "ensgId", col("s") as "name", col("uniprot"), col("HGNC"), col("symbols"))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
  }

  def addEnsemblIdsToUniprot(hgnc: Dataset[Hgnc], uniprot: DataFrame): DataFrame = {
    logger.debug("Grouping Uniprot entries by Ensembl Id.")
    hgnc
      .select(col("ensemblId"), explode(col("uniprotIds")).as("uniprotId"))
      .withColumn("id", col("uniprotId"))
      .withColumn("source", typedLit("uniprot_obsolete"))
      .transform(nest(_, List("id", "source"), "uniprotProteinId"))
      .join(uniprot, Seq("uniprotId"))
      .groupBy("ensemblId")
      .agg(
        flatten(collect_set(col("synonyms"))).as("synonyms"),
        flatten(collect_set(col("symbolSynonyms"))).as("symbolSynonyms"),
        flatten(collect_set(col("nameSynonyms"))).as("nameSynonyms"),
        flatten(collect_set(col("functionDescriptions"))).as("functionDescriptions"),
        flatten(collect_set(col("proteinIds"))).as("proteinIds"),
        flatten(collect_set(col("subcellularLocations"))).as("subcellularLocations"),
        flatten(collect_set(col("dbXrefs"))).as("dbXrefs"),
        collect_set(col("uniprotProteinId")).as("uniprotProteinId"),
        flatten(collect_set(col("targetClass"))).as("targetClass")
      )
      .withColumn(
        "targetClass",
        when(size(col("targetClass")) < 1, null).otherwise(col("targetClass"))
      )
      .withColumnRenamed("ensemblId", "id")
      .withColumn("proteinIds", safeArrayUnion(col("proteinIds"), col("uniprotProteinId")))
      .drop("uniprotId", "uniprotProteinId")
  }

  private def removeRedundantXrefs(dataFrame: DataFrame): DataFrame = {
    logger.info("Removing redundant references from target ouput")
    val cols = dataFrame.columns.filter(_ != "dbXrefs") :+
      "filter(dbXrefs, struct -> struct.source != 'GO' and struct.source != 'Ensembl') as dbXrefs"
    dataFrame.selectExpr(cols: _*)
  }

  private def addTep(tep: DataFrame, idLookup: DataFrame)(dataFrame: DataFrame): DataFrame = {
    logger.info("Adding TEP to target dataframe.")

    val lut =
      idLookup
        .select(col("ensgId").as("id"), col("symbols"))
        .withColumn("symbol", explode(col("symbols")))
        .drop("symbols")
        .orderBy(col("symbol").asc)

    val tepFields: List[Column] = classOf[Tep].getDeclaredFields.map(f => col(f.getName)).toList
    val tepWithEnsgId = tep
      .join(lut, lut("symbol") === tep("targetFromSourceId"), "inner")
      .withColumn("tep", struct(tepFields: _*))
      .select("id", "tep")
      .orderBy(col("id").asc)
      .cache()

    dataFrame.join(tepWithEnsgId, Seq("id"), "left_outer")
  }

  private def addOrthologue(
      orthologue: Dataset[Ortholog]
  )(dataFrame: DataFrame)(implicit ss: SparkSession): DataFrame = {
    logger.info("Adding Homologues to dataframe")

    /** The orthologs should appear in the order of closest to further from homosapiens. See opentargets/platform#1699
      */
    ss.udf.register(
      "speciesDistanceSort",
      (o: Ortholog, o1: Ortholog) => {
        o.priority compare o1.priority
      }
    )

    // add in gene symbol for paralogs (human genes)
    val geneSymbols = dataFrame
      .select(col("id"), col("approvedSymbol"))
      .cache()

    val homoDF = orthologue
      .join(broadcast(geneSymbols), Seq("id"))
      .join(
        broadcast(
          geneSymbols
            .withColumnRenamed("approvedSymbol", "paralogGeneSymbol")
            .withColumnRenamed("id", "paralogId")
        ),
        col("paralogId") === col("targetGeneId"),
        "left_outer"
      )
      .withColumn(
        "targetGeneSymbol",
        coalesce(col("paralogGeneSymbol"), col("targetGeneSymbol"), col("approvedSymbol"))
      )
      .drop("approvedSymbol", "paralogGeneSymbol", "paralogId")

    val groupedById = nest(homoDF, homoDF.columns.filter(_ != "id").toList, "homologues")
      .groupBy("id")
      .agg(collect_list("homologues") as "homologues")
      .selectExpr("id", "array_sort(homologues, (x, y) -> speciesDistanceSort(x, y)) as homologues")

    dataFrame
      .join(groupedById, Seq("id"), "left_outer")
      .drop("humanGeneId")
  }

  private def addTractability(
      tractability: Dataset[TractabilityWithId]
  )(dataFrame: DataFrame): DataFrame = {
    logger.info("Adding Tractability to dataframe")
    dataFrame
      .join(tractability, col("ensemblGeneId") === col("id"), "left_outer")
      .drop("ensemblGeneId")
  }

  private def addNcbiEntrezSynonyms(ncbi: Dataset[Ncbi])(dataFrame: DataFrame): DataFrame = {
    logger.info("Adding ncbi entrez synonyms to dataframe")

    val ncbiF = ncbi
      .withColumnRenamed("synonyms", "s")
      .withColumnRenamed("nameSynonyms", "ns")
      .withColumnRenamed("symbolSynonyms", "ss")

    dataFrame
      .join(ncbiF, Seq("id"), "left_outer")
      .withColumn("symbolSynonyms", safeArrayUnion(col("symbolSynonyms"), col("ss")))
      .withColumn("nameSynonyms", safeArrayUnion(col("nameSynonyms"), col("ns")))
      .withColumn("synonyms", safeArrayUnion(col("synonyms"), col("s"), col("ns"), col("ss")))
      .drop("s", "ss", "ns")
  }

  private def addTargetSafety(inputDataFrames: IOResources, geneToEnsemblLookup: DataFrame)(
      dataFrame: DataFrame
  )(implicit sparkSession: SparkSession): DataFrame = {
    val tsDS: Dataset[TargetSafety] = Safety(
      inputDataFrames("safetyAE").data,
      inputDataFrames("safetySR").data,
      inputDataFrames("safetyTox").data,
      geneToEnsemblLookup
    )
    logger.info("Adding target safety to dataframe")
    dataFrame.join(tsDS, Seq("id"), "left_outer")
  }

  private def addReactome(
      reactomeDataDf: Dataset[Reactomes]
  )(interimTargetDf: DataFrame): DataFrame = {
    logger.info("Adding reactome pathways to dataframe")
    interimTargetDf.join(reactomeDataDf, Seq("id"), "left_outer")
  }

  /** Group chemical probes by ensembl ID and add to interim target dataframe.
    *
    * @param cpDF              raw chemical probes dataset provided by PIS
    * @param ensemblIdLookupDF map from ensg -> other names.
    * @param targetDF          interim target dataset
    * @return target dataset with chemical probes added
    */
  private def addChemicalProbes(cpDF: DataFrame, ensemblIdLookupDF: DataFrame)(
      targetDF: DataFrame
  ): DataFrame = {
    logger.info("Add chemical probes to target.")
    val cpWithEnsgId = cpDF
      .join(ensemblIdLookupDF, array_contains(col("name"), col("targetFromSourceId")))
      .drop(ensemblIdLookupDF.columns.filter(_ != "ensgId"): _*)

    val cpGroupedById = cpWithEnsgId
      .select(
        col("ensgId") as "id",
        struct(
          cpDF.columns.filterNot(_ == "ensgId").map(col): _*
        ) as "probe"
      )
      .groupBy(col("id"))
      .agg(
        collect_list(col("probe")) as "chemicalProbes"
      )
    targetDF.join(broadcast(cpGroupedById), Seq("id"), "left_outer")
  }

  /** Return map on input IOResources */
  private def getMappedInputs(
      targetConfig: Configuration.Target
  )(implicit sparkSession: SparkSession): Map[String, IOResource] = {
    def getUniprotDataFrame(io: IOResourceConfig)(implicit ss: SparkSession): IOResource = {
      import ss.implicits._
      val path: java.util.Iterator[String] = sparkSession.read.textFile(io.path).toLocalIterator()
      val data = UniprotConverter.fromFlatFile(path.asScala)
      IOResource(data.toDF(), io)
    }

    val targetInputs = targetConfig.input
    val mappedInputs = Map(
      "chembl" -> targetInputs.chembl,
      "chemicalProbes" -> targetInputs.chemicalProbes,
      "ensembl" -> targetInputs.ensembl,
      "geneticConstraints" -> targetInputs.geneticConstraints,
      "geneOntologyHuman" -> targetInputs.geneOntology,
      "geneOntologyRna" -> targetInputs.geneOntologyRna,
      "geneOntologyRnaLookup" -> targetInputs.geneOntologyRnaLookup,
      "geneOntologyEcoLookup" -> targetInputs.geneOntologyEco,
      "hallmarks" -> targetInputs.hallmarks,
      "hgnc" -> targetInputs.hgnc,
      "homologyCodingProteins" -> targetInputs.homologyCodingProteins,
      "homologyDictionary" -> targetInputs.homologyDictionary,
      "homologyGeneDictionary" -> targetInputs.homologyGeneDictionary,
      "hpa" -> targetInputs.hpa,
      "hpaSL" -> targetInputs.hpaSlOntology,
      "ncbi" -> targetInputs.ncbi.copy(options = targetInputs.ncbi.options match {
        case Some(value) => Option(value)
        case None        => CsvHelpers.tsvWithHeader
      }),
      "projectScoresIds" -> targetInputs.psGeneIdentifier,
      "projectScoresEssentialityMatrix" -> targetInputs.psEssentialityMatrix,
      "reactomeEtl" -> targetInputs.reactomeEtl,
      "reactomePathways" -> targetInputs.reactomePathways,
      "safetyAE" -> targetInputs.safetyAdverseEvent,
      "safetySR" -> targetInputs.safetySafetyRisk,
      "safetyTox" -> targetInputs.safetyToxicity.copy(
        options = targetInputs.safetyToxicity.options match {
          case Some(value) => Option(value)
          case None        => CsvHelpers.tsvWithHeader
        }
      ),
      "tep" -> targetInputs.tep,
      "tractability" -> targetInputs.tractability,
      "uniprotSsl" -> targetInputs.uniprotSsl
    )

    IoHelpers
      .readFrom(mappedInputs)
      .updated(
        "uniprot",
        getUniprotDataFrame(
          IOResourceConfig(
            targetInputs.uniprot.format,
            targetInputs.uniprot.path
          )
        )
      )
  }

  private def addProteinClassificationToUniprot(
      uniprot: Dataset[Uniprot],
      proteinClassification: Dataset[ProteinClassification]
  ): DataFrame = {
    logger.debug("Add protein classifications to UniprotDS")
    val proteinClassificationWithUniprot = uniprot
      .select(
        col("uniprotId"),
        explode(array_union(array(col("uniprotId")), col("proteinIds.id"))) as "pid"
      )
      .withColumn("pid", trim(col("pid")))
      .join(proteinClassification, col("pid") === proteinClassification("accession"), "left_outer")
      .drop("accession")
      .groupBy("uniprotId")
      .agg(flatten(collect_set(col("targetClass"))).as("targetClass"))

    uniprot.join(proteinClassificationWithUniprot, Seq("uniprotId"), "left_outer")
  }

  /** Merge Hgnc and Ensembl datasets in a way that preserves logic of data pipeline.
    *
    * The deprecated data pipeline build up the target dataset in a step-wise manner, where later steps only added
    * fields if they were not already provided by an earlier one. This method reproduces that logic so that fields
    * provided on both datasets are set by Hgnc.
    */
  private def mergeHgncAndEnsembl(hgnc: Dataset[Hgnc], ensembl: Dataset[Ensembl]): DataFrame = {
    logger.debug("Merging Hgnc and Ensembl datasets")
    val eDf = ensembl
      .withColumnRenamed("approvedName", "an")
      .withColumnRenamed("approvedSymbol", "as")

    val merged = eDf
      // this removes non-reference ensembl genes introduced by HGNC.
      .join(hgnc, eDf("id") === hgnc("ensemblId"), "left_outer")
      // if approvedName and approvedSymbol provided by HGNC use those, otherwise Ensembl for symbol or empty string for name
      .withColumn("approvedName", coalesce(col("approvedName"), col("an"), lit("")))
      .withColumn("approvedSymbol", coalesce(col("approvedSymbol"), col("as"), col("id")))
      .drop("an", "as")
    logger.debug(
      s"Merged HGNC and Ensembl dataframe has columns: ${merged.columns.mkString("Array(", ", ", ")")}"
    )
    // todo check what needs to happen to merge unused approvedNames...probably go to symbol synonyms.
    merged
  }

  /** Updates column `proteinIds` to remove duplicates and sort by source.
    *
    * @return dataframe with same schema as input.
    */
  def filterAndSortProteinIds(dataFrame: DataFrame): DataFrame = {
    val splitToken = "-"
    val ensemblId = "eid"
    val proteinId = "proteinIds"
    val df = dataFrame
      .select(col("id").as(ensemblId), explode(col(proteinId)).as("pid"))
      .select(col(ensemblId), col("pid.*"))
      .withColumn("array", array(trim(col("id")), col("source")))
      .withColumn("array", array_join(col("array"), splitToken))
      .groupBy(ensemblId)
      .agg(collect_list("array").as("accessions"))
      .select(col(ensemblId), proteinIdUdf(col("accessions")).as("accessions"))
      .select(col(ensemblId), explode(col("accessions")).as("accessions"))
      .withColumn("idAndSource", split(col("accessions"), splitToken))
      .select(
        col(ensemblId),
        col("idAndSource").getItem(0).as("id"),
        col("idAndSource").getItem(1).as("source")
      )
      .select(col(ensemblId), struct(col("id"), col("source")).as(proteinId))
      .groupBy(ensemblId)
      .agg(collect_list(proteinId).as(proteinId))
      .withColumnRenamed(ensemblId, "id")

    dataFrame.drop(proteinId).join(df, Seq("id"), "left_outer")

  }

  /** Removes duplicate proteinIds and orders output by source preference
    *
    * The ETL collects proteinIds (accession numbers) from a variety of sources and combines them all. This function
    * removes duplicate accession numbers. The duplicate to remove is determined by the following source hierarchy:
    *
    * 1. uniprot_swissprot
    * 2. uniprot_trembl
    * 3. uniprot
    * 4. ensembl_PRO
    *
    * ids with entries in the form ACCESSION-SOURCE. Accession and Source are split by a hyphen.
    * @return input array with duplicates removed and sorted by hierarchy preference.
    */
  val cleanProteinIds: Seq[String] => Seq[String] = (ids: Seq[String]) => {
    val splitToken = "-"
    val map = scala.collection.mutable.Map[String, String]()
    val idAndSource: Seq[String] = ids
      .map(entry => entry.split(splitToken))
      .map(arr => (arr.head.trim, arr.tail.head))
      .foldLeft(Seq.empty[String])((acc, nxt) => {
        if (map.contains(nxt._1)) acc
        else {
          map.update(nxt._1, nxt._2)
          (nxt._1 + splitToken + nxt._2) +: acc
        }
      })
    idAndSource.sortWith((left, right) => {
      val sourceToInt = (source: String) =>
        source match {
          case swissprot if swissprot.contains("swiss") => 1
          case trembl if trembl.contains("trembl")      => 2
          case uniprot if uniprot.endsWith("uniprot")   => 3
          case ensembl if ensembl.contains("ensembl")   => 4
          case _                                        => 5
        }
      val l = sourceToInt(left)
      val r = sourceToInt(right)
      // if same source use natural ordering otherwise custom ordering for source.
      if (l == r) left < right else l < r
    })
  }
  val proteinIdUdf: UserDefinedFunction = udf(cleanProteinIds)

}
