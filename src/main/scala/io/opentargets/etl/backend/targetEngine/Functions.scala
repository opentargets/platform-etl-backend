package io.opentargets.etl.backend.targetEngine

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object Functions extends LazyLogging {

  def targetMembraneQuery(
      querysetDF: DataFrame,
      targetsDF: DataFrame,
      parentChildCousinsDF: DataFrame
  ): DataFrame = {
    val sourceList = Seq("HPA_1", "HPA_secreted", "HPA_add_1", "uniprot_1", "uniprot_secreted")

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

    val locationInfoDF = targetsDF
      .select(col("id").as("targetid"), explode_outer(col("subcellularLocations")))
      .select(col("targetid"),
              when(col("col.location").isNull, lit("noInfo"))
                .otherwise("hasInfo")
                .as("result")
      )
      .dropDuplicates()

    val membraneGroupedDF = targetsDF
      .select(col("*"), explode_outer(col("subcellularLocations")))
      .select(col("id"), col("col.*"))
      .select(
        col("*"),
        when(
          (col("source") === "HPA_main")
            && col("termSL").isin(membraneTerms: _*),
          lit("HPA_1")
        )
          .when(
            col("source") === "HPA_extracellular_location",
            lit("HPA_secreted")
          )
          .when(
            (col("source") === "HPA_additional")
              && (col("termSL").isin(membraneTerms: _*)),
            lit("HPA_add_1")
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
      .select(col("id").as("targetid"), col("Count_mb"), col("source"))
      .dropDuplicates(Seq("targetid", "Count_mb"))
      .groupBy("targetid")
      .agg(
        array_distinct(collect_list("Count_mb")).as("mb"),
        count(col("source")).as("counted")
      )

    val membraneWithLocDF = membraneGroupedDF
      .select(
        col("*"),
        when(
          ((array_contains(col("mb"), "HPA_secreted")
            && array_contains(col("mb"), "uniprot_secreted")))
            && (col("counted") == 2),
          lit("onlySecreted")
        )
          .when(
            ((array_contains(col("mb"), "HPA_secreted")
              || array_contains(col("mb"), "uniprot_secreted")))
              && (col("counted") == 1),
            lit("onlySecreted")
          ) // refactor to merge with prev cond
          .when(((array_contains(col("mb"), "HPA_secreted")
                  && array_contains(col("mb"), "uniprot_secreted")))
                  && (col("counted") > 2),
                lit("secreted&inMembrane")
          )
          .otherwise(lit("inMembrane"))
          .as("loc")
      )

    val membraneJoinLoc = membraneWithLocDF
      .join(locationInfoDF, Seq("targetid"), "right")

    val ligandDF = membraneJoinLoc
      .select(
        col("*"),
        when((col("loc") === "secreted&inMembrane") ||
               (col("loc") === "inMembrane"),
             lit(1)
        )
          .when(
            (
              (col("loc") =!= "inMembrane") ||
                (col("loc") =!= "secreted&inMembrane")
            ) &&
              (col("result") ===
                "hasInfo"),
            lit(0)
          )
          .when(col("result") === "noInfo", lit(null))
          .otherwise(lit(0))
          .as("Nr_mb")
      )

    val secretedDF = ligandDF.select(
      col("*"),
      when((col("loc") === "secreted&inMembrane") ||
             (col("loc") === "onlySecreted"),
           lit(1)
      )
        .when(
          (
            (col("loc") =!= "onlySecreted") ||
              (col("loc") =!= "secreted&inMembrane")
          ) &&
            (col("result") ===
              "hasInfo"),
          lit(0)
        )
        .when(col("result") === "noInfo", lit(null))
        .otherwise(lit(0))
        .as("Nr_secreted")
    )

    querysetDF.join(secretedDF, Seq("targetid"), "left")

  }

  def biotypeQuery(biotypeDF: DataFrame, targetsDF: DataFrame): DataFrame = {
    val prDF = targetsDF
      .select(
        col("id").as("targetid"),
        col("biotype"),
        when(col("biotype") === "protein_coding", 1)
          otherwise (0)
          as ("Nr_biotype")
      )
      .dropDuplicates() // is this necessary?

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
          .as("Nr_Pocket")
      )

    val joinedDF = querySetDF
      .join(filteredTargetsDF, Seq("targetid"), "left")

    joinedDF
  }

  def safetyQuery(querySetDF: DataFrame, targetsDF: DataFrame): DataFrame = {
    val aggEventsDF = targetsDF
      .select(
        col("*"),
        explode_outer(col("safetyLiabilities")),
        when(col("safetyLiabilities") =!= array(), lit("conInfo"))
          .otherwise(lit("noReported"))
          .as("info")
      )
      .select(col("id").as("targetid"), col("col.*"), col("info"))
      .groupBy(col("targetid"), col("info"))
      .agg(
        count(col("event")).as("nEvents"),
        array_distinct(collect_list("event")).as("events")
      )

    val hasInfo = (col("nEvents") > 0) && (col("info") === "conInfo")

    val eventsDF =
      aggEventsDF.select(
        col("*"),
        when(hasInfo, lit(-1))
          .otherwise(lit(null))
          .as("Nr_Event")
      )

    querySetDF.join(eventsDF, Seq("targetid"), "left")
  }

  def constraintQuery(querySetDF: DataFrame, targetsDF: DataFrame): DataFrame = {
    val contraintsDF = targetsDF
      .select(col("id").as("targetid"), explode(col("constraint")))
      .select(col("targetid"), col("col.*"))
      .filter(col("constraintType") === "lof")
      .select(col("targetid"),
              ((col("upperRank") - 9456) / 19196).as("cal_score"),
              col("constraintType")
      )

    querySetDF.join(contraintsDF, Seq("targetid"), "left")
  }

  def paralogsQuery(querySetDF: DataFrame, targetsDF: DataFrame): DataFrame = {
    val explodedDF = targetsDF
      .select(col("id").as("targetid"),
              when(col("homologues") =!= array(), lit("hasInfo"))
                .otherwise("noInfo/null")
                .as("hasInfo"),
              explode(col("homologues"))
      )
      .withColumn("homoType", split(col("col.homologyType"), "_").getItem(0))
      .withColumn("howmany", split(col("col.homologyType"), "_").getItem(1))
      .withColumn("homoType", regexp_replace(col("homoType"), "other", "paralog_other"))
      .withColumn(
        "homoType",
        regexp_replace(col("homoType"), "within", "paralog_intrasp")
      )

    val paralogDF = explodedDF
      .select("targetid", "homoType", "howmany", "hasInfo", "col.queryPercentageIdentity")
      .filter(col("homoType").contains("paralog"))
      .groupBy("targetid")
      .agg(
        max("queryPercentageIdentity").as("max")
      )
      .select(col("*"),
              when(col("max") <
                     60,
                   lit(0)
              )
                .when(col("max") >=
                        60,
                      lit(
                        -(((col("max") - 60) *
                          100) /
                          40)
                      )
                )
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
          .when(col("max") >= 80, lit(((col("max") - 80) * 100) / 20))
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
      when(col("tep.description") =!= null, lit(1)).otherwise(lit(null)).as("Nr_TEP")
    )
    querySetDF.join(tepDF, Seq("targetid"), "left")
  }

  def mousemodQuery(querySetDF: DataFrame, mouseDF: DataFrame): DataFrame = {
    val mouseModelsDF = mouseDF
      .select(
        col("targetFromSourceId"),
        explode(col("modelPhenotypeClasses")).as("classes"),
        col("classes.label")
      )
      .select(
        col("targetFromSourceId").as("target_id_"),
        col("classes.label")
      )
      .groupBy("target_id_")
      .agg(
        count("label").as("Nr_mouse_models"),
        collect_set("label").as("Different_PhenoClasses")
      )
      .select(
        col("*"),
        when(col("Nr_mouse_models") =!= "0", lit(1))
          .otherwise(lit(0))
          .as("Nr_Mousemodels")
      )

    querySetDF
      .join(mouseModelsDF, col("target_id_") === querySetDF.col("targetid"), "left")
  }

  def chemicalProbesQuery(querySetDF: DataFrame, targetsDF: DataFrame): DataFrame = {
    val probesDF = targetsDF
      .select(
        col("id").as("targetid"),
        explode_outer(col("chemicalProbes")),
        when(size(col("chemicalProbes")) > 0, lit("hasInfo"))
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

    val clinTrials = Seq(0, 1, 2, 3)

    val drugApprovedDF = moleculeDF
      .select(
        col("id").as("drug_id"),
        col("maximumClinicalTrialPhase"),
        col("linkedTargets"),
        when(col("maximumClinicalTrialPhase") === 4, lit(1))
          .when(col("maximumClinicalTrialPhase").isin(clinTrials: _*), lit(0))
          .otherwise(lit(null))
          .as("Nr_ClinTrial")
      )

    val drugActionDF = moleculeMecDF.select(
      col("actionType"),
      explode_outer(col("chemblIds")).as("chembl")
    )

    val appdrugTargetsDF =
      drugApprovedDF
        .join(drugActionDF, drugActionDF.col("chembl") === drugApprovedDF.col("drug_id"), "left")
        .withColumn(
          "ClinTrials",
          concat_ws(
            "_",
            col("chembl"),
            lit("ClinTrialPhase"),
            col("maximumClinicalTrialPhase")
          )
        )
        .withColumn("targets", explode_outer(col("linkedTargets.rows")))
        .select(
          "targets",
          "chembl",
          "actionType",
          "maximumClinicalTrialPhase",
          "ClinTrials",
          "Nr_ClinTrial"
        )
        .dropDuplicates(Seq("targets", "chembl"))
        .groupBy("targets")
        .agg(
          collect_list("ClinTrials").as("ChEMBL&ClinTrialPhase"),
          collect_list("actionType").as("App_drug_actionType"),
          max("Nr_ClinTrial").as("maxClinTrialPhase")
        )

    querySetDF
      .join(appdrugTargetsDF, col("targetid") === appdrugTargetsDF.col("targets"), "left")

  }

  def tissueSpecificQuery(querySetDF: DataFrame, hpaDataDF: DataFrame): DataFrame = {

    val hpaDF =
      hpaDataDF
        .select(
          col("Ensembl"),
          col("RNA tissue specificity").as("Tissue_specificity_RNA"),
          col("RNA tissue distribution").as("Tissue_distribution_RNA"),
          when(col("RNA tissue specificity") === "Tissue enriched", lit(1))
            .when(col("RNA tissue specificity") === "Group enriched", lit(0.5))
            .when(col("RNA tissue specificity") === "Tissue enhanced", lit(0))
            .when(col("RNA tissue specificity") === "Low tissue specificity", lit(-1))
            .when(col("RNA tissue specificity") === "Not detected", lit(null))
            .as("Nr_specificity"),
          when(col("RNA tissue distribution") === "Detected in single", lit(1))
            .when(col("RNA tissue distribution") === "Detected in some", lit(0.5))
            .when(col("RNA tissue distribution") === "Detected in many", lit(0))
            .when(col("RNA tissue distribution") === "Detected in all", lit(-1))
            .when(col("RNA tissue distribution") === "Not detected", lit(null))
            .as("Nr_distribution")
        )

    querySetDF.join(hpaDF, col("targetid") === hpaDF.col("Ensembl"), "left")

  }
}
