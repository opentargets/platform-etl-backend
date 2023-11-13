package io.opentargets.etl.backend.targetEngine

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object Functions extends LazyLogging {

  /** Harmonic sum UDFs Harmonic sum expressions and their associated spark UDFs To calculate the
    * harmonic sum of a series of scores:
    *   1. sort scores in descending order 2. divide each score by its 1-based index squared 3. sum
    *      all values
    */
  val harmonicSum: Array[Double] => Double = (scores: Array[Double]) => {
    val sortedScores = scores.sorted(Ordering[Double].reverse)
    val denominators = (1 to sortedScores.length).map(i => math.pow(i, 2)).toArray
    val harmonicSum = sortedScores.zip(denominators).map(i => i._1 / i._2).sum
    harmonicSum
  }
  val harmonic_sum_udf: UserDefinedFunction = udf(harmonicSum)
  val maxHarmonicSum: Array[Double] => Double = (scores: Array[Double]) => {
    val maxScores = scores.map(_ => 1d)
    harmonicSum(maxScores)
  }
  val max_harmonic_sum_udf: UserDefinedFunction = udf(maxHarmonicSum)
  val scaledHarmonicSum: (Double, Double) => Double = (harmonicSum: Double, maximum: Double) =>
    harmonicSum / maximum
  val scaled_harmonic_sum_udf: UserDefinedFunction = udf(scaledHarmonicSum)

  def targetMembraneQuery(
      querysetDF: DataFrame,
      targetsDF: DataFrame,
      parentChildCousinsDF: DataFrame
  ): DataFrame = {

    val membraneTerms =
      parentChildCousinsDF
        .filter(col("Name") === "Cell membrane")
        .select(explode(col("toSearch")).as("termSL"))
        .collect()
        .map(va => va.getString(0))
        .toSeq

    val secretedTerms =
      parentChildCousinsDF
        .filter(col("Name") === "Secreted")
        .select(explode(col("toSearch")).as("termSL"))
        .collect()
        .map(va => va.getString(0))
        .toSeq

    val subcellularLocationsDF = targetsDF
      .select(col("id").as("targetid"), explode_outer(col("subcellularLocations")))

    val locationInfoDF = subcellularLocationsDF
      .select(col("targetid"),
              when(col("col.location").isNull, lit("noInfo"))
                .otherwise("hasInfo")
                .as("result")
      )
      .dropDuplicates()

    // TODO: Evaluate deleting this list and change the filter to exclude everything no info
    val sourceList =
      Seq("HPA_1", "HPA_secreted", "HPA_add_1", "uniprot_1", "uniprot_secreted", "HPA_dif")

    val membraneGroupedDF = subcellularLocationsDF
      .select(col("targetid"), col("col.*"))
      .select(
        col("*"),
        when(
          (col("source") === "HPA_main")
            && col("termSL").isin(membraneTerms: _*),
          lit("HPA_1")
        )
          .when(
            (col("source") === "HPA_main")
              && not(col("termSL").isin(membraneTerms: _*)),
            lit("HPA_dif")
          )
          .when(
            col("source") === "HPA_extracellular_location",
            lit("HPA_secreted")
          )
          .when(
            (col("source") === "HPA_additional")
              && col("termSL").isin(membraneTerms: _*),
            lit("HPA_add_1")
          )
          .when(
            (col("source") === "HPA_additional")
              && not(col("termSL").isin(membraneTerms: _*)),
            lit("HPA_dif")
          )
          .when(
            (col("source") === "uniprot")
              && col("termSL").isin(membraneTerms: _*),
            lit("uniprot_1")
          )
          .when(
            (col("source") === "uniprot")
              && (col("termSL").isin(secretedTerms: _*)),
            lit("uniprot_secreted")
          )
          .otherwise(lit("Noinfo"))
          .as("Count_mb")
      )
      .filter(col("Count_mb").isin(sourceList: _*))
      .select(col("targetid"), col("Count_mb"), col("source"))
      .dropDuplicates(Seq("targetid", "Count_mb"))
      .groupBy("targetid")
      .agg(
        collect_set("Count_mb").as("mb"),
        count(col("source")).as("counted")
      )

    val proteinClassificationDF = membraneGroupedDF
      .select(
        col("*"),
        when(array_contains(col("mb"), "HPA_1")
               || array_contains(col("mb"), "HPA_add_1"),
             lit("yes")
        )
          .when(array_contains(col("mb"), "HPA_dif"), lit("dif"))
          .otherwise(lit("no"))
          .as("HPA_membrane"),
        when(array_contains(col("mb"), "HPA_secreted"), lit("yes"))
          .otherwise(lit("no"))
          .as("HPA_secreted"),
        when(array_contains(col("mb"), "uniprot_1"), lit("yes"))
          .otherwise(lit("no"))
          .as("uniprot_membrane"),
        when(array_contains(col("mb"), "uniprot_secreted"), lit("yes"))
          .otherwise(lit("no"))
          .as("uniprot_secreted")
      )

    // TODO: Improve DF name
    val membraneWithLocDF = proteinClassificationDF
      .select(
        col("*"),
        when((col("HPA_membrane") === "yes")
               && (col("HPA_secreted") === "no"),
             lit("inMembrane")
        )
          .when(
            (col("HPA_membrane") === "no" || col("HPA_membrane") === "dif")
              && col("HPA_secreted") === "yes",
            lit("onlySecreted")
          )
          .when(
            (col("HPA_membrane") === "yes")
              && (col("HPA_secreted") === "yes"),
            lit("secreted&inMembrane")
          )
          .when(
            col("HPA_membrane") === "no"
              && col("HPA_secreted") === "no",
            when(
              col("uniprot_membrane") === "yes"
                && col("uniprot_secreted") === "no",
              lit("inMembrane")
            )
              .when(
                col("uniprot_membrane") === "no"
                  && col("uniprot_secreted") === "yes",
                lit("onlySecreted")
              )
              .when(
                col("uniprot_membrane") === "yes"
                  && col("uniprot_secreted") === "yes",
                lit("secreted&inMembrane")
              )
          )
          .when(col("HPA_membrane") === "dif", lit("noMembraneHPA"))
          .as("loc")
      )

    val joinedDF = membraneWithLocDF
      .join(querysetDF, Seq("targetid"), "right")
      .join(locationInfoDF, Seq("targetid"), "left")

    joinedDF.select(
      col("*"),
      when(
        (col("loc") === "secreted&inMembrane")
          || (col("loc") === "inMembrane"),
        lit(1)
      )
        .when(
          (col("loc") =!= "secreted&inMembrane")
            || (col("loc") =!= "inMembrane"),
          lit(0)
        )
        .when(col("loc").isNull
                && (col("result") === "hasInfo"),
              lit(0)
        )
        .as("Nr_mb"),
      when(
        (col("loc") === "secreted&inMembrane")
          || (col("loc") === "onlySecreted"),
        lit(1)
      )
        .when(
          col("loc") === "inMembrane"
            && col("result") === "hasInfo",
          lit(0)
        )
        .when(
          (col("loc") =!= "onlySecreted"
            || col("loc") =!= "secreted&inMembrane")
            && (col("result") === "hasInfo"),
          lit(0)
        )
        .when(col("result") === "noInfo", lit(null))
        .otherwise(lit(0))
        .as("Nr_secreted")
    )

  }

  def biotypeQuery(biotypeDF: DataFrame, targetsDF: DataFrame): DataFrame = {
    val prDF = targetsDF
      .select(
        col("id").as("targetid"),
        col("biotype"),
        when(col("biotype") === "protein_coding", 1)
          .when(col("biotype") === "", null)
          otherwise (0)
          as ("Nr_biotype")
      )

    prDF.join(biotypeDF, Seq("targetid"), "left")
  }

  def ligandPocketQuery(querySetDF: DataFrame, targetsDF: DataFrame): DataFrame = {
    val filteredTargetsDF = targetsDF
      .select(
        col("id").as("targetid"),
        explode_outer(col("tractability")).as("new_struct") // this turns into multiple rows
      )
      .filter(
        col("new_struct.id") === "High-Quality Ligand"
          || col("new_struct.id") === "High-Quality Pocket"
          || col("new_struct.id") === "Small Molecule Binder"
      )
      .select(col("*"),
              col("new_struct").getItem("id").as("type"),
              col("new_struct").getItem("value").cast(IntegerType).as("presence")
      )
      .groupBy(col("targetid"))
      .pivot("type")
      .agg(sum("presence"))
      .select(
        col("*"),
        when(col("High-Quality Ligand") === 1, lit(1))
          .otherwise(lit(0))
          .as("Nr_Ligand"),
        when(col("High-Quality Pocket") === 1, lit(1))
          .otherwise(lit(0))
          .as("Nr_Pocket"),
        when(col("Small Molecule Binder") === 1, lit(1))
          .otherwise(lit(0))
          .as("Nr_sMBinder")
      )

    val joinedDF = querySetDF
      .join(filteredTargetsDF, Seq("targetid"), "left")

    joinedDF
  }

  def safetyQuery(querySetDF: DataFrame, targetsDF: DataFrame): DataFrame = {
    val aggEventsDF = targetsDF
      .withColumn(
        "info",
        when(size(col("safetyLiabilities")) > 0, lit("conInfo"))
          .otherwise(lit("noReported"))
      )
      .select(
        col("id").as("targetid"),
        explode_outer(col("safetyLiabilities")),
        col("info")
      )
      .groupBy("targetid", "info")
      .agg(
        count(col("col.event")).as("nEvents"),
        array_distinct(collect_list("col.event")).as("events")
      )
      .withColumn(
        "hasSafetyEvent",
        when(
          (col("nEvents") > 0) && (col("info") === "conInfo"),
          lit("Yes")
        ).otherwise(lit(null))
      )
      .withColumn(
        "Nr_Event",
        when(col("hasSafetyEvent") === "Yes", lit(-1))
          .otherwise(lit(null))
      )

    querySetDF.join(aggEventsDF, Seq("targetid"), "left")
  }

  def constraintQuery(querySetDF: DataFrame, targetsDF: DataFrame): DataFrame = {

    val constraints = (
      targetsDF
        .select(col("id").as("constr_id"), explode(col("constraint")))
        .select(col("col.*"))
        .filter(col("constraintType") === "lof")
        .groupBy("constraintType")
        .agg(
          min("upperRank").as("lowerRank"),
          max("upperRank").as("upperRank")
        )
        .select(
          col("lowerRank").cast(IntegerType),
          col("upperRank").cast(IntegerType)
        )
      )
      .first()

    val minUpperRank = constraints.getInt(0)
    val maxUpperRank = constraints.getInt(1)

    val contraintsDF = targetsDF
      .select(col("id").as("targetid"), explode(col("constraint")))
      .select(col("targetid"), col("col.*"))
      .filter(col("constraintType") === "lof")
      .select(
        col("targetid"),
        lit((lit(2) * ((col("upperRank") - minUpperRank) / (maxUpperRank - minUpperRank))) - lit(1))
          .as("cal_score"),
        col("constraintType")
      )

    querySetDF.join(contraintsDF, Seq("targetid"), "left")
  }

  def paralogsQuery(querySetDF: DataFrame, targetsDF: DataFrame): DataFrame = {

    def replaceOther: Column => Column = (homoType) =>
      regexp_replace(homoType, "other", "paralog_other")
    def replaceWithin: Column => Column = (homoType) =>
      regexp_replace(homoType, "within", "paralog_intrasp")

    val explodedDF = targetsDF
      .select(col("id").as("targetid"),
              when(size(col("homologues")) > lit(0), lit("hasInfo"))
                .otherwise("noInfo/null")
                .as("hasInfo"),
              explode(col("homologues"))
      )
      .select(
        col("targetid"),
        replaceWithin(replaceOther(split(col("col.homologyType"), "_").getItem(0)))
          .as("homoType"),
        split(col("col.homologyType"), "_")
          .getItem(1)
          .as("howmany"),
        col("hasInfo"),
        col("col.queryPercentageIdentity")
      )

    val overSixty = lit(-((col("max") - lit(60)) / lit(40)))

    val paralogDF = explodedDF
      .filter(col("homoType").contains("paralog"))
      .groupBy("targetid")
      .agg(
        max("queryPercentageIdentity").as("max")
      )
      .select(col("*"),
              when(col("max") < lit(60), lit(0))
                .when(col("max") >= lit(60), overSixty)
                .as("Nr_paralogs")
      )

    querySetDF.join(paralogDF, Seq("targetid"), "left")
  }

  def orthologsMouseQuery(querySetDF: DataFrame, targetsDF: DataFrame): DataFrame = {
    val orthologsDF = targetsDF
      .select(col("id").as("targetid"), explode(col("homologues")))
      .select(col("targetid"), col("col.*"))
      .select(col("*"),
              split(col("homologyType"), "_").getItem(0).as("homoType"),
              split(col("homologyType"), "_").getItem(1).as("howmany")
      )
      .filter(
        (col("homoType").contains("ortholog"))
          && (col("speciesName") === "Mouse")
      )
      .select(
        "targetid",
        "homoType",
        "howmany",
        "targetGeneid",
        "targetPercentageIdentity",
        "queryPercentageIdentity"
      )
      .groupBy("targetid")
      .agg(
        max("queryPercentageIdentity").as("max")
      )
      .select(
        col("*"),
        when(col("max") < 80, lit(0))
          .when(col("max") >= 80, lit((col("max") - 80) / 20))
          .as("Nr_ortholog")
      )

    querySetDF.join(orthologsDF, Seq("targetid"), "left")
  }

  def driverGeneQuery(querySetDF: DataFrame, targetsDF: DataFrame): DataFrame = {
    val oncotsgList = Seq("TSG",
                          "oncogene",
                          "Oncogene",
                          "oncogene",
                          "oncogene,TSG",
                          "TSG,oncogene",
                          "fusion,oncogene",
                          "oncogene,fusion"
    )

    val oncoTargetsDF = targetsDF
      .select(col("id").as("targetid"), explode_outer(col("hallmarks.attributes")))
      .select(
        col("targetid"),
        col("col.description"),
        when(col("col.description").isin(oncotsgList: _*), lit(1))
          .otherwise(lit(0))
          .as("annotation")
      )
      .groupBy("targetid")
      .agg(
        max(col("annotation")).as("counts")
      )
      .select(
        col("*"),
        when(col("counts") =!= 0, lit(-1))
          .otherwise(lit(null))
          .as("Nr_CDG")
      )
    querySetDF.join(oncoTargetsDF, Seq("targetid"), "left")
  }

  def tepQuery(querySetDF: DataFrame, targetsDF: DataFrame): DataFrame = {
    val tepDF = targetsDF.select(
      col("id").as("targetid"),
      col("tep.*"),
      when((col("tep.description") isNotNull) or col("tep.description") =!= "", lit(1))
        .otherwise(lit(null))
        .as("Nr_TEP")
    )
    querySetDF.join(tepDF, Seq("targetid"), "left")
  }

  def mousemodQuery(querySetDF: DataFrame,
                    mouseDF: DataFrame,
                    mousePhenoScoresDF: DataFrame
  ): DataFrame = {
    val phenoScoresDF = mousePhenoScoresDF
      .select(
        col("id").as("idLabel"),
        col("score")
      )
      .withColumn(
        "score",
        when(col("score") === 0.0, lit(0)).otherwise(lit(col("score")))
      )
    val mouseModelsDF = mouseDF
      .select(
        col("targetFromSourceId").as("target_id_"),
        explode_outer(col("modelPhenotypeClasses")).as("classes"),
        col("classes.label"),
        col("classes.id")
      )
      .join(
        phenoScoresDF,
        col("classes.id") === phenoScoresDF.col("idLabel"),
        "left"
      )
      .withColumn("score", col("score").cast("double"))
      .groupBy("target_id_")
      .agg(collect_list(col("score")).alias("score"))
      .withColumn("harmonicSum", harmonic_sum_udf(col("score")).cast("double"))
      .withColumn("maxHarmonicSum", max_harmonic_sum_udf(col("score")).cast("double"))
      .withColumn("maximum", max(col("maxHarmonicSum")).over(Window.orderBy()).cast("double"))
      .withColumn("scaledHarmonicSum", -scaled_harmonic_sum_udf(col("harmonicSum"), col("maximum")))
    querySetDF
      .join(mouseModelsDF, col("target_id_") === querySetDF.col("targetid"), "left")

  }

  def chemicalProbesQuery(querySetDF: DataFrame, targetsDF: DataFrame): DataFrame = {
    val probesDF = targetsDF
      .select(
        col("id").as("targetid"),
        explode_outer(col("chemicalProbes")),
        when(size(col("chemicalProbes")) > lit(0), lit("hasInfo"))
          .otherwise(lit("noInfo"))
          .as("info")
      )

    val grouped = probesDF
      .select(
        col("*"),
        when(col("info") === "hasInfo"
               && col("col.isHighQuality") === "true",
             lit(1)
        )
          .when(col("info") === "hasInfo"
                  && col("col.isHighQuality") === "false",
                lit(0)
          )
          .otherwise(lit(null))
          .as("Nr_chprob")
      )
      .groupBy("targetid")
      .agg(max(col("Nr_chprob")).as("Nr_chprob"))

    querySetDF.join(grouped, Seq("targetid"), "left")
  }

  def clinTrialsQuery(querySetDF: DataFrame,
                      moleculeDF: DataFrame,
                      moleculeMecDF: DataFrame
  ): DataFrame = {

    val drugApprovedDF = moleculeDF
      .select(
        col("id").as("drug_id"),
        explode_outer(col("linkedTargets.rows")).as("targets"),
        col("maximumClinicalTrialPhase")
      )
      .dropDuplicates("targets", "maximumClinicalTrialPhase")
      .groupBy("targets")
      .agg(
        max("maximumClinicalTrialPhase").as("maxClinTrialPhase")
      )
      .select(
        col("*"),
        when(col("maxClinTrialPhase") >= 0, col("maxClinTrialPhase") / 4)
          .otherwise(null)
          .as("inClinicalTrials")
      )

    querySetDF
      .join(drugApprovedDF, col("targetid") === drugApprovedDF.col("targets"), "left")

  }

  def tissueSpecificQuery(querySetDF: DataFrame, hpaDataDF: DataFrame): DataFrame = {

    val hpaDF =
      hpaDataDF
        .select(
          "Ensembl",
          "RNA tissue distribution",
          "RNA tissue specificity",
          "Antibody"
        )
        .withColumnRenamed("RNA tissue distribution", "Tissue_distribution_RNA")
        .withColumnRenamed("RNA tissue specificity", "Tissue_specificity_RNA")
        .select("Ensembl", "Tissue_specificity_RNA", "Tissue_distribution_RNA")
        .withColumn(
          "Nr_specificity",
          when(
            col("Tissue_specificity_RNA") === "Tissue enriched",
            lit(1)
          )
            .when(
              col("Tissue_specificity_RNA") === "Group enriched",
              lit(0.75)
            )
            .when(col("Tissue_specificity_RNA") === "Tissue enhanced", lit(0.5))
            .when(
              col("Tissue_specificity_RNA") === "Low tissue specificity",
              lit(-1)
            )
            .when(col("Tissue_specificity_RNA") === "Not detected", lit(null))
        )
        .withColumn(
          "Nr_distribution",
          when(
            col("Tissue_distribution_RNA") === "Detected in single",
            lit(1)
          )
            .when(
              col("Tissue_distribution_RNA") === "Detected in some",
              lit(0.5)
            )
            .when(
              col("Tissue_distribution_RNA") === "Detected in many",
              lit(0)
            )
            .when(col("Tissue_distribution_RNA") === "Detected in all", lit(-1))
            .when(col("Tissue_distribution_RNA") === "Not detected", lit(null))
        )

    querySetDF.join(hpaDF, col("targetid") === hpaDF.col("Ensembl"), "left")

  }
}
