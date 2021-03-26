package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers.{mkFlattenArray, nest, safeArrayUnion, validateDF}
import io.opentargets.etl.backend.{Configuration, ETLSessionContext}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import io.opentargets.etl.backend.spark.{CsvHelpers, IOResource, IOResourceConfig, IoHelpers}
import io.opentargets.etl.preprocess.uniprot.UniprotConverter
import org.apache.spark.sql.functions.{
  array,
  array_join,
  array_union,
  coalesce,
  col,
  collect_list,
  collect_set,
  explode,
  flatten,
  split,
  struct,
  trim,
  typedLit,
  udf
}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

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
    val hgnc: Dataset[Hgnc] = Hgnc(inputDataFrames("hgnc").data)
    val ncbi: Dataset[Ncbi] = Ncbi(inputDataFrames("ncbi").data)
    val ensemblDf: Dataset[Ensembl] = Ensembl(inputDataFrames("ensembl").data)
    val uniprotDS: Dataset[Uniprot] = Uniprot(inputDataFrames("uniprot").data)
    val geneOntologyDf: Dataset[GeneOntologyByEnsembl] = GeneOntology(
      inputDataFrames("geneOntologyHuman").data,
      inputDataFrames("geneOntologyRna").data,
      inputDataFrames("geneOntologyRnaLookup").data,
      ensemblDf)
    val tep: Dataset[TepWithId] = Tep(inputDataFrames("tep").data)
    val hpa: Dataset[GeneWithLocation] = GeneWithLocation(inputDataFrames("hpa").data)
    val projectScoresDS: Dataset[GeneWithDbXRef] = ProjectScores(
      inputDataFrames("projectScoresIds").data,
      inputDataFrames("projectScoresEssentialityMatrix").data)
    val proteinClassification: Dataset[ProteinClassification] = ProteinClassification(
      inputDataFrames("chembl").data)
    val geneticConstraints: Dataset[GeneticConstraintsWithId] = GeneticConstraints(
      inputDataFrames("geneticConstraints").data)
    val homology: Dataset[LinkedOrtholog] = Ortholog(
      inputDataFrames("orthologs").data,
      inputDataFrames("homologyDictionary").data,
      inputDataFrames("homologyCodingProteins").data,
      inputDataFrames("homologyNcRna").data,
      context.configuration.target.hgncOrthologSpecies
    )
    val tractability: Dataset[TractabilityWithId] = Tractability(
      inputDataFrames("tractability").data)

    // 3. merge intermediate data frames into final
    val hgncEnsemblTepGoDF = mergeHgncAndEnsembl(hgnc, ensemblDf)
      .join(tep, ensemblDf("id") === tep("ensemblId"), "left_outer")
      .drop("ensemblId")
      .join(geneOntologyDf, ensemblDf("id") === geneOntologyDf("ensemblId"), "left_outer")
      .drop("ensemblId")
      .join(projectScoresDS, Seq("id"), "left_outer")

    val uniprotGroupedByEnsemblIdDF =
      addEnsemblIdsToUniprot(hgnc,
                             addProteinClassificationToUniprot(uniprotDS, proteinClassification))
        .withColumnRenamed("proteinIds", "pid")
        .join(hpa, Seq("id"), "left_outer")
        .withColumn("subcellularLocations",
                    mkFlattenArray(col("subcellularLocations"), col("locations")))
        .drop("locations")

    hgncEnsemblTepGoDF
      .join(uniprotGroupedByEnsemblIdDF, Seq("id"), "left_outer")
      .withColumn("proteinIds", safeArrayUnion(col("proteinIds"), col("pid")))
      .withColumn("dbXrefs",
                  safeArrayUnion(col("hgncId"), col("dbXrefs"), col("signalP"), col("xRef")))
      .withColumn("synonyms", safeArrayUnion(col("synonyms"), col("hgncSynonyms")))
      .drop("pid", "hgncId", "hgncSynonyms", "uniprotIds", "signalP", "xRef")
      .join(geneticConstraints, Seq("id"), "left_outer")
      .transform(filterAndSortProteinIds)
      .transform(removeRedundantXrefs)
      .transform(addOrthologue(homology))
      .transform(addTractability(tractability))
      .transform(addNcbiEntrezSynonyms(ncbi))

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
        flatten(collect_set(col("functionDescriptions"))).as("functionDescriptions"),
        flatten(collect_set(col("proteinIds"))).as("proteinIds"),
        flatten(collect_set(col("subcellularLocations"))).as("subcellularLocations"),
        flatten(collect_set(col("dbXrefs"))).as("dbXrefs"),
        collect_set(col("uniprotProteinId")).as("uniprotProteinId"),
        flatten(collect_set(col("targetClass"))).as("targetClass")
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

  private def addOrthologue(orthologue: Dataset[LinkedOrtholog])(
      dataFrame: DataFrame): DataFrame = {
    logger.info("Adding Homologues to dataframe")
    dataFrame
      .join(orthologue, col("humanGeneId") === col("id"), "left_outer")
      .drop("humanGeneId")
  }

  private def addTractability(tractability: Dataset[TractabilityWithId])(
      dataFrame: DataFrame): DataFrame = {
    logger.info("Adding Tractability to dataframe")
    dataFrame
      .join(tractability, col("ensemblGeneId") === col("id"), "left_outer")
      .drop("ensemblGeneId")
  }

  private def addNcbiEntrezSynonyms(ncbi: Dataset[Ncbi])(dataFrame: DataFrame): DataFrame = {
    logger.info("Adding ncbi entrez synonyms to dataframe")
    dataFrame
      .join(ncbi.withColumnRenamed("synonyms", "s"), Seq("id"), "left_outer")
      .withColumn("synonyms", safeArrayUnion(col("synonyms"), col("s")))
      .drop("s")
  }

  /** Return map on input IOResources */
  private def getMappedInputs(targetConfig: Configuration.Target)(
      implicit sparkSession: SparkSession): Map[String, IOResource] = {
    def getUniprotDataFrame(io: IOResourceConfig)(implicit ss: SparkSession): IOResource = {
      import ss.implicits._
      val path: java.util.Iterator[String] = sparkSession.read.textFile(io.path).toLocalIterator()
      val data = UniprotConverter.convertUniprotFlatFileToUniprotEntry(path.asScala)
      IOResource(data.toDF(), io)
    }

    val targetInputs = targetConfig.input
    val mappedInputs = Map(
      "chembl" -> IOResourceConfig(
        targetInputs.chembl.format,
        targetInputs.chembl.path
      ),
      "ensembl" -> IOResourceConfig(
        targetInputs.ensembl.format,
        targetInputs.ensembl.path
      ),
      "geneticConstraints" -> IOResourceConfig(
        targetInputs.geneticConstraints.format,
        targetInputs.geneticConstraints.path,
        options = targetInputs.geneticConstraints.options
      ),
      "geneOntologyHuman" -> IOResourceConfig(
        targetInputs.geneOntology.format,
        targetInputs.geneOntology.path,
        options = targetInputs.geneOntology.options
      ),
      "geneOntologyRna" -> IOResourceConfig(
        targetInputs.geneOntologyRna.format,
        targetInputs.geneOntologyRna.path,
        options = targetInputs.geneOntologyRna.options
      ),
      "geneOntologyRnaLookup" -> IOResourceConfig(
        targetInputs.geneOntologyRnaLookup.format,
        targetInputs.geneOntologyRnaLookup.path,
        options = targetInputs.geneOntologyRnaLookup.options
      ),
      "hgnc" -> IOResourceConfig(
        targetInputs.hgnc.format,
        targetInputs.hgnc.path
      ),
      "homologyCodingProteins" -> IOResourceConfig(
        targetInputs.homologyCodingProteins.format,
        targetInputs.homologyCodingProteins.path,
        options = targetInputs.homologyCodingProteins.options
      ),
      "homologyDictionary" -> IOResourceConfig(
        targetInputs.homologyDictionary.format,
        targetInputs.homologyDictionary.path,
        options = targetInputs.homologyDictionary.options
      ),
      "homologyNcRna" -> IOResourceConfig(
        targetInputs.homologyNcRna.format,
        targetInputs.homologyNcRna.path,
        options = targetInputs.homologyNcRna.options
      ),
      "hpa" -> IOResourceConfig(
        targetInputs.hpa.format,
        targetInputs.hpa.path,
        options = targetInputs.hpa.options
      ),
      "ncbi" -> IOResourceConfig(
        targetInputs.ncbi.format,
        targetInputs.ncbi.path,
        options =
          if (targetInputs.ncbi.options.isDefined) targetInputs.ncbi.options
          else CsvHelpers.tsvWithHeader
      ),
      "orthologs" -> IOResourceConfig(
        targetInputs.ortholog.format,
        targetInputs.ortholog.path,
        options = CsvHelpers.tsvWithHeader
      ),
      "projectScoresIds" -> IOResourceConfig(
        targetInputs.psGeneIdentifier.format,
        targetInputs.psGeneIdentifier.path,
        options = targetInputs.psGeneIdentifier.options
      ),
      "projectScoresEssentialityMatrix" -> IOResourceConfig(
        targetInputs.psEssentialityMatrix.format,
        targetInputs.psEssentialityMatrix.path,
        options = targetInputs.psEssentialityMatrix.options
      ),
      "tep" -> IOResourceConfig(
        targetInputs.tep.format,
        targetInputs.tep.path
      ),
      "tractability" -> IOResourceConfig(
        targetInputs.tractability.format,
        targetInputs.tractability.path,
        options = targetInputs.tractability.options
      )
    )

    IoHelpers
      .readFrom(mappedInputs)
      .updated("uniprot",
               getUniprotDataFrame(
                 IOResourceConfig(
                   targetInputs.uniprot.format,
                   targetInputs.uniprot.path
                 )))
  }

  private def addProteinClassificationToUniprot(
      uniprot: Dataset[Uniprot],
      proteinClassification: Dataset[ProteinClassification]): DataFrame = {
    logger.debug("Add protein classifications to UniprotDS")
    val proteinClassificationWithUniprot = uniprot
      .select(col("uniprotId"), col("proteinIds.id").as("pid"))
      .withColumn("uid", array(col("uniprotId")))
      .withColumn("pid", array_union(col("uid"), col("pid")))
      .select(col("uniprotId"), explode(col("pid")).as("pid"))
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
      // if approvedName and approvedSymbol provided by HGNC use those, otherwise Ensembl.
      .withColumn("approvedName", coalesce(col("approvedName"), col("an"), typedLit("")))
      .withColumn("approvedSymbol", coalesce(col("approvedSymbol"), col("as"), typedLit("")))
      .drop("an", "as")
    logger.debug(
      s"Merged HGNC and Ensembl dataframe has columns: ${merged.columns.mkString("Array(", ", ", ")")}")
    // todo check what needs to happen to merge unused approvedNames...probably go to symbol synonyms.
    merged
  }

  /** Updates column `proteinIds` to remove duplicates and sort by source.
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
      .select(col(ensemblId),
              col("idAndSource").getItem(0).as("id"),
              col("idAndSource").getItem(1).as("source"))
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
    * @param ids with entries in the form ACCESSION-SOURCE. Accession and Source are split by a hyphen.
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
  val proteinIdUdf = udf(cleanProteinIds)

}
