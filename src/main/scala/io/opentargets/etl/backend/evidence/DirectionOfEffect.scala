package io.opentargets.etl.backend.evidence

import io.opentargets.etl.backend.ETLSessionContext
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object DirectionOfEffect {
  implicit class DoEColumnUtilities(val col: Column) {
    def isRiskSourceCol(source: String): Column = col.when(
      col("datasourceId") === source,
      when(col("diseaseId").isNotNull, lit("risk")).otherwise(
        lit(null)
      )
    )
    def isProtectSourceCol(source: String): Column = col.when(
      col("datasourceId") === source,
      when(col("diseaseId").isNotNull, lit("protect")).otherwise(
        lit(null)
      )
    )
  }

  private def geneProductLevel(whenDecrease: Column, whenIncrease: Column): Column = when(
    col("variantFunctionalConsequenceFromQtlId")
      === "SO_0002316",
    whenDecrease
  )
    .when(
      col("variantFunctionalConsequenceFromQtlId")
        === "SO_0002315",
      whenIncrease
    )

  private val betaValidation = when(
    col("beta").isNotNull && col("OddsRatio").isNull,
    when(col("beta") > 0, lit("risk"))
      .when(col("beta") < 0, lit("protect"))
      .otherwise(lit(null))
  )
    .when(
      col("beta").isNull && col("OddsRatio").isNotNull,
      when(col("OddsRatio") > 1, lit("risk"))
        .when(col("OddsRatio") < 1, lit("protect"))
        .otherwise(lit(null))
    )
    .when(
      col("beta").isNull && col("OddsRatio").isNull,
      lit(null)
    )
    .when(
      col("beta").isNotNull && col("OddsRatio").isNotNull,
      lit(null)
    )

  private val clinicalSignificancesValidation = when(
    col("clinicalSignificances").rlike("(pathogenic)$"),
    lit("risk")
  )
    .when(
      col("clinicalSignificances").contains("protect"),
      lit("protect")
    )
    .otherwise(
      lit(null)
    )

  def apply(evidencesDF: DataFrame, targetsDF: DataFrame, mechanismsOfActionDF: DataFrame)(implicit
      context: ETLSessionContext
  ): DataFrame = {
    val evidenceConfig = context.configuration.evidences
    val actionTypeDF = mechanismsOfActionDF
      .select(
        explode_outer(col("chemblIds")).as("drugId2"),
        col("actionType"),
        col("mechanismOfAction"),
        col("targets")
      )
      .select(
        explode_outer(col("targets")).as("targetId2"),
        col("drugId2"),
        col("actionType"),
        col("mechanismOfAction")
      )
      .groupBy("targetId2", "drugId2")
      .agg(
        collect_set("actionType").as("actionType")
      )

    val oncotsgList = evidenceConfig.directionOfEffect.oncotsgList;

    val oncolabelDF = (
      targetsDF
        .select(
          col("id"),
          col("approvedSymbol"),
          explode_outer(col("hallmarks.attributes"))
        )
        .select("id", "approvedSymbol", "col.description")
        .filter(col("description").isin(oncotsgList: _*))
        .groupBy("id", "approvedSymbol")
        .agg(collect_set("description").alias("description"))
        .withColumn("description_splited", concat_ws(",", col("description")))
        .withColumn(
          "TSorOncogene",
          when(
            (
              col("description_splited").rlike("ncogene")
                && col("description_splited").rlike("TSG")
            ),
            lit(null)
          )
            .when(col("description_splited").rlike("ncogene(\\s|$)"), lit("oncogene"))
            .when(col("description_splited").rlike("TSG(\\s|$)"), lit("TSG"))
            .otherwise(lit(null))
        )
        .withColumnRenamed(
          "id",
          "target_id"
        )
    )

    directionOfEffectFunc(evidencesDF, oncolabelDF, actionTypeDF)
  }

  def directionOfEffectFunc(evidencesDF: DataFrame,
                            oncolabelDF: DataFrame,
                            actionTypeDF: DataFrame
  )(implicit context: ETLSessionContext): DataFrame = {
    val evidenceConfig = context.configuration.evidences;
    val gof = evidenceConfig.directionOfEffect.gof;
    val lof = evidenceConfig.directionOfEffect.lof;
    val filterLof = evidenceConfig.directionOfEffect.varFilterLof;
    val inhibitors = evidenceConfig.directionOfEffect.inhibitors;
    val activators = evidenceConfig.directionOfEffect.activators;
    val sources = evidenceConfig.directionOfEffect.sources;

    val validEvidencesDF = evidencesDF.filter(col("datasourceId").isin(sources: _*))

    val windowSpec = Window.partitionBy("targetId", "diseaseId")

    def intogenFunction(): Column =
      when(arrays_overlap(
             col("mutatedSamples.functionalConsequenceId"),
             array(gof map lit: _*)
           ),
           lit("GoF")
      ).when(arrays_overlap(
               col("mutatedSamples.functionalConsequenceId"),
               array(lof map lit: _*)
             ),
             lit("LoF")
      )

    val variantIsLoF = col("variantFunctionalConsequenceId").isin(filterLof: _*)

    val variantIsGoF = col("variantFunctionalConsequenceId").isin(gof: _*)

    val joinedDF = validEvidencesDF
      .withColumn(
        "beta",
        col("beta").cast("float")
      ) // ot genetics & gene burden
      .withColumn(
        "oddsRatio",
        col("oddsRatio").cast("float")
      ) // ot genetics & gene burden
      .withColumn(
        "clinicalSignificances",
        concat_ws(",", col("clinicalSignificances"))
      ) // eva
      .join(oncolabelDF, oncolabelDF.col("target_id") === col("targetId"), "left") // cgce_burden
      .join(
        actionTypeDF, // chembl
        (actionTypeDF.col("drugId2") === col("drugId"))
          && (actionTypeDF.col("targetId2") === col("targetId")),
        "left"
      )

    // variant Effect Column
    joinedDF
      .withColumn("inhibitors_list", array(inhibitors map lit: _*))
      .withColumn("activators_list", array(activators map lit: _*))
      .withColumn(
        "intogen_function",
        intogenFunction()
      )
      .withColumn(
        "intogenAnnot",
        size(collect_set(col("intogen_function")).over(windowSpec))
      )
      .withColumn(
        "variantEffect",
        when(
          col("datasourceId") === "ot_genetics_portal",
          when(
            col("variantFunctionalConsequenceId").isNotNull,
            when(
              col("variantFunctionalConsequenceFromQtlId").isNull,
              when(
                variantIsLoF,
                lit("LoF")
              )
                .when(
                  variantIsGoF,
                  lit("GoF")
                )
                .otherwise(lit(null))
            )
              // variantFunctionalConsequenceFromQtlId
              .when(
                col("variantFunctionalConsequenceFromQtlId").isNotNull,
                when(
                  variantIsLoF,
                  geneProductLevel(whenDecrease = lit("LoF"), whenIncrease = lit(null))
                    .otherwise(lit("LoF"))
                ).when(
                  not(variantIsLoF), // when is not a LoF, still can be a GoF
                  when(
                    not(variantIsGoF), // if not GoF
                    geneProductLevel(whenDecrease = lit("LoF"), whenIncrease = lit("GoF"))
                      .otherwise(lit(null))
                  ).when(
                    variantIsGoF, // if is GoF
                    geneProductLevel(whenDecrease = lit(null), whenIncrease = lit("GoF"))
                  )
                )
              )
          ).when(
            col("variantFunctionalConsequenceId").isNull,
            geneProductLevel(whenDecrease = lit("LoF"), whenIncrease = lit("GoF"))
              .otherwise(lit(null))
          )
        ).when(
          col("datasourceId") === "gene_burden",
          when(col("targetId").isNotNull, lit("LoF")).otherwise(
            lit(null)
          )
        )
          // # Eva_germline
          .when(
            col("datasourceId") === "eva",
            when(
              variantIsLoF,
              lit("LoF")
            ).otherwise(
              lit(null)
            )
            // Son todas aquellas que tenen info pero no son LoF
          )
          // # Eva_somatic
          .when(
            col("datasourceId") === "eva_somatic",
            when(
              variantIsLoF,
              lit("LoF")
            ).otherwise(
              lit(null)
            ) // Son todas aquellas que tenen info pero no son patogenicas / protective + LoF
          )
          // # G2P
          .when(
            col("datasourceId")
              === "gene2phenotype", // 6 types of variants[SO_0002318, SO_0002317, SO_0001622, SO_0002315, SO_0001566, SO_0002220]
            when(
              col("variantFunctionalConsequenceId") === "SO_0002317",
              lit("LoF")
            ) // absent gene product
              .when(
                col("variantFunctionalConsequenceId") === "SO_0002315",
                lit("GoF")
              ) // increased gene product level
              .otherwise(lit(null))
          )
          // # Orphanet
          .when(
            col("datasourceId") === "orphanet",
            when(
              col("variantFunctionalConsequenceId") === "SO_0002054",
              lit("LoF")
            ) // Loss of Function Variant
              .when(
                col("variantFunctionalConsequenceId") === "SO_0002053",
                lit("GoF")
              ) // Gain_of_Function Variant
              .otherwise(lit(null))
          )
          // # CGC
          .when(
            col("datasourceId") === "cancer_gene_census",
            when(col("TSorOncogene") === "oncogene", lit("GoF"))
              .when(col("TSorOncogene") === "TSG", lit("LoF"))
              .when(col("TSorOncogene").isNull, lit(null))
              .otherwise(null)
          )
          // # intogen
          .when(
            col("datasourceId") === "intogen",
            when(
              col("intogenAnnot") === 1,
              intogenFunction()
            )
              .when(col("intogenAnnot") > 1, lit(null))
              .otherwise(lit(null))
          )
          // # impc
          .when(
            col("datasourceId") === "impc",
            when(col("diseaseId").isNotNull, lit("LoF")).otherwise(
              lit(null)
            )
          )
          // chembl
          .when(
            col("datasourceId") === "chembl",
            when(size(array_intersect(col("actionType"), col("inhibitors_list"))) >= 1, lit("LoF"))
              .when(size(array_intersect(col("actionType"), col("activators_list"))) >= 1,
                    lit("GoF")
              )
              .otherwise(lit(null))
          )
      )
      .withColumn(
        "directionOnTrait",
        // ot genetics portal
        when(
          col("datasourceId")
            === "ot_genetics_portal", // the same for gene_burden
          betaValidation
        ).when(
          col("datasourceId") === "gene_burden",
          betaValidation
        )
          // Eva_germline
          .when(
            col("datasourceId") === "eva", // the same for eva_somatic
            clinicalSignificancesValidation
          )
          // # Eva_somatic
          .when(
            col("datasourceId") === "eva_somatic",
            clinicalSignificancesValidation
          )
          // # G2P
          .isRiskSourceCol("gene2phenotype")
          // # Orphanet
          .isRiskSourceCol("orphanet")
          // # CGC
          .isRiskSourceCol("cancer_gene_census")
          // # intogen
          .isRiskSourceCol("intogen")
          // # impc
          .isRiskSourceCol("impc")
          // chembl
          .isProtectSourceCol("chembl")
      )
      .withColumn(
        "homogenizedVersion",
        when(
          (col("variantEffect") === "LoF")
            && (col("directionOnTrait") === "risk"),
          lit("LoF_risk")
        )
          .when(
            (col("variantEffect") === "LoF")
              && (col("directionOnTrait") === "protect"),
            lit("LoF_protect")
          )
          .when(
            (col("variantEffect") === "GoF")
              && (col("directionOnTrait") === "risk"),
            lit("GoF_risk")
          )
          .when(
            (col("variantEffect") === "GoF")
              && (col("directionOnTrait") === "protect"),
            lit("GoF_protect")
          )
          .otherwise(lit(null))
      )

  }

}
