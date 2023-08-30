package io.opentargets.etl.backend.evidence

import io.opentargets.etl.backend.ETLSessionContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object DirectionOfEffect {

  def apply(evidencesDF: DataFrame,
            targetsDF: DataFrame,
            mechanismsOfActionDF: DataFrame,
            geneBurdenDF: DataFrame
  )(implicit context: ETLSessionContext): DataFrame = {
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
      .dropDuplicates()

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
            lit("bivalent")
          )
            .when(col("description_splited").rlike("ncogene(\\s|$)"), lit("oncogene"))
            .when(col("description_splited").rlike("TSG(\\s|$)"), lit("TSG"))
            .otherwise(lit("noEvaluable"))
        )
        .withColumnRenamed(
          "id",
          "target_id"
        )
    )

    directionOfEffectFunc(evidencesDF, oncolabelDF, geneBurdenDF, actionTypeDF)
  }

  def directionOfEffectFunc(evidencesDF: DataFrame,
                            oncolabelDF: DataFrame,
                            geneBurdenDF: DataFrame,
                            actionTypeDF: DataFrame
  )(implicit context: ETLSessionContext): DataFrame = {
    val geneRenamedDF = geneBurdenDF.withColumnRenamed("statisticalMethodOverview", "stMethod")
    val evidenceConfig = context.configuration.evidences;
    val gof = evidenceConfig.directionOfEffect.gof;
    val lof = evidenceConfig.directionOfEffect.lof;
    val filterLof = evidenceConfig.directionOfEffect.varFilterLof;
    val inhibitors = evidenceConfig.directionOfEffect.inhibitors;
    val activators = evidenceConfig.directionOfEffect.activators;

    val joinedDF = evidencesDF
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
      .join(oncolabelDF, oncolabelDF.col("target_id") === col("targetId"), "left") // cgc
      .join(
        geneRenamedDF,
        geneRenamedDF.col("stMethod") === col("statisticalMethodOverview"),
        "left"
      ) // gene_burden
      .join(
        actionTypeDF, // chembl
        (actionTypeDF.col("drugId2") === col("drugId"))
          && (actionTypeDF.col("targetId2") === col("targetId")),
        "left"
      )

    // variant Effect Column
    joinedDF
      .withColumn(
        "variantEffect",
        when(
          col("datasourceId") === "ot_genetics_portal",
          when(
            col("variantFunctionalConsequenceId").isNotNull,
            when(
              col("variantFunctionalConsequenceFromQtlId").isNull,
              when(
                col("variantFunctionalConsequenceId").isin(
                  filterLof: _*
                ),
                lit("LoF")
              )
                .when(
                  col("variantFunctionalConsequenceId").isin(gof: _*),
                  lit("GoF")
                )
                .otherwise(lit("noEvaluable"))
            )
              // variantFunctionalConsequenceFromQtlId
              .when(
                col("variantFunctionalConsequenceFromQtlId").isNotNull,
                when(
                  col("variantFunctionalConsequenceId").isin(
                    filterLof: _*
                  ), // when is a LoF variant
                  when(
                    col("variantFunctionalConsequenceFromQtlId")
                      === "SO_0002316",
                    lit("LoF")
                  )
                    .when(
                      col("variantFunctionalConsequenceFromQtlId")
                        === "SO_0002315",
                      lit("conflict/noEvaluable")
                    )
                    .otherwise(lit("LoF"))
                ).when(
                  col("variantFunctionalConsequenceId").isin(filterLof: _*)
                    === false, // when is not a LoF, still can be a GoF
                  when(
                    col("variantFunctionalConsequenceId").isin(gof: _*)
                      === false, // if not GoF
                    when(
                      col("variantFunctionalConsequenceFromQtlId")
                        === "SO_0002316",
                      lit("LoF")
                    )
                      .when(
                        col("variantFunctionalConsequenceFromQtlId")
                          === "SO_0002315",
                        lit("GoF")
                      )
                      .otherwise(lit("noEvaluable"))
                  ).when(
                    col("variantFunctionalConsequenceId").isin(
                      gof: _*
                    ), // if is GoF
                    when(
                      col("variantFunctionalConsequenceFromQtlId")
                        === "SO_0002316",
                      lit("conflict/noEvaluable")
                    ).when(
                      col("variantFunctionalConsequenceFromQtlId")
                        === "SO_0002315",
                      lit("GoF")
                    )
                  )
                )
              )
          ).when(
            col("variantFunctionalConsequenceId").isNull,
            when(
              col("variantFunctionalConsequenceFromQtlId") === "SO_0002316",
              lit("LoF")
            )
              .when(
                col("variantFunctionalConsequenceFromQtlId") === "SO_0002315",
                lit("GoF")
              )
              .otherwise(lit("noEvaluable"))
          )
        ).when(
          col("datasourceId") === "gene_burden",
          when(col("whatToDo") === "get", lit("LoF")).otherwise(
            lit("noEvaluable")
          ) // son tambien no data las que tiene riesgo pero no se ensayan LoF o PT
        )
          // # Eva_germline
          .when(
            col("datasourceId") === "eva",
            when(
              col("variantFunctionalConsequenceId").isin(filterLof: _*),
              lit("LoF")
            ).otherwise(
              lit("noEvaluable")
            )
            // Son todas aquellas que tenen info pero no son LoF
          )
          // # Eva_somatic
          .when(
            col("datasourceId") === "eva_somatic",
            when(
              col("variantFunctionalConsequenceId").isin(filterLof: _*),
              lit("LoF")
            ).otherwise(
              lit("noEvaluable")
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
              .otherwise(lit("noEvaluable"))
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
              .otherwise(lit("noEvaluable"))
          )
          // # CGC
          .when(
            col("datasourceId") === "cancer_gene_census",
            when(col("TSorOncogene") === "oncogene", lit("GoF"))
              .when(col("TSorOncogene") === "TSG", lit("LoF"))
              .when(col("TSorOncogene") === "bivalent", lit("bivalent"))
              .otherwise("noEvaluable")
          )
          // # intogen
          .when(
            col("datasourceId") === "intogen",
            when(
              arrays_overlap(col("mutatedSamples.functionalConsequenceId"),
                             array(gof.map(lit): _*)
              ),
              lit("GoF")
            )
              .when(
                arrays_overlap(
                  col("mutatedSamples.functionalConsequenceId"),
                  array(lof.map(lit): _*)
                ),
                lit("LoF")
              )
              .otherwise(lit("noEvaluable"))
          )
          // # impc
          .when(
            col("datasourceId") === "impc",
            when(col("diseaseId").isNotNull, lit("LoF")).otherwise(
              lit("noEvaluable")
            )
          )
          // chembl
          .when(
            col("datasourceId") === "chembl",
            when(col("actionType").isin(inhibitors: _*), lit("LoF"))
              .when(col("actionType").isin(activators: _*), lit("GoF"))
              .otherwise(lit("noEvaluable"))
          )
      )
      .withColumn(
        "directionOnTrait",
        // ot genetics portal
        when(
          col("datasourceId")
            === "ot_genetics_portal", // the same for gene_burden
          when(
            (col("beta").isNotNull) && (col("OddsRatio").isNull),
            when(col("beta") > 0, lit("risk"))
              .when(col("beta") < 0, lit("protect"))
              .otherwise(lit("noEvaluable"))
          )
            .when(
              (col("beta").isNull) && (col("OddsRatio").isNotNull),
              when(col("OddsRatio") > 1, lit("risk"))
                .when(col("OddsRatio") < 1, lit("protect"))
                .otherwise(lit("noEvaluable"))
            )
            .when(
              (col("beta").isNull) && (col("OddsRatio").isNull),
              lit("noEvaluable")
            )
            .when(
              (col("beta").isNotNull) && (col("OddsRatio").isNotNull),
              lit("conflict/noEvaluable")
            )
        ).when(
          col("datasourceId") === "gene_burden",
          when(
            (col("beta").isNotNull) && (col("OddsRatio").isNull),
            when(col("beta") > 0, lit("risk"))
              .when(col("beta") < 0, lit("protect"))
              .otherwise(lit("noEvaluable"))
          )
            .when(
              (col("oddsRatio").isNotNull) && (col("beta").isNull),
              when(col("oddsRatio") > 1, lit("risk"))
                .when(col("oddsRatio") < 1, lit("protect"))
                .otherwise(lit("noEvaluable"))
            )
            .when(
              (col("beta").isNull) && (col("oddsRatio").isNull),
              lit("noEvaluable")
            )
            .when(
              (col("beta").isNotNull) && (col("oddsRatio").isNotNull),
              lit("conflict")
            )
        )
          // Eva_germline
          .when(
            col("datasourceId") === "eva", // the same for eva_somatic
            when(
              col("clinicalSignificances").rlike("(pathogenic)$"),
              lit("risk")
            )
              .when(
                col("clinicalSignificances").contains("protect"),
                lit("protect")
              )
              .otherwise(
                lit("noEvaluable")
              )
            // Son todas aquellas que tenen info pero no son patogenicas / protective + LoF
          )
          // # Eva_somatic
          .when(
            col("datasourceId") === "eva_somatic",
            when(
              col("clinicalSignificances").rlike("(pathogenic)$"),
              lit("risk")
            )
              .when(
                col("clinicalSignificances").contains("protect"),
                lit("protect")
              )
              .otherwise(
                lit("noEvaluable")
              ) // Son todas aquellas que tenen info pero no son patogenicas / protective + LoF
          )
          // # G2P
          .when(
            col("datasourceId") === "gene2phenotype",
            when(col("diseaseId").isNotNull, lit("risk")).otherwise(
              lit("noEvaluable")
            )
          )
          // # Orphanet
          .when(
            col("datasourceId") === "orphanet",
            when(col("diseaseId").isNotNull, lit("risk")).otherwise(
              lit("noEvaluable")
            )
          )
          // # CGC
          .when(
            col("datasourceId") === "cancer_gene_census",
            when(col("diseaseId").isNotNull, lit("risk")).otherwise(
              lit("noEvaluable")
            )
          )
          // # intogen
          .when(
            col("datasourceId") === "intogen",
            when(col("diseaseId").isNotNull, lit("risk")).otherwise(
              lit("noEvaluable")
            )
          )
          // # impc
          .when(
            col("datasourceId") === "impc",
            when(col("diseaseId").isNotNull, lit("risk")).otherwise(
              lit("noEvaluable")
            )
          )
          // chembl
          .when(
            col("datasourceId") === "chembl",
            when(col("diseaseId").isNotNull, lit("protect")).otherwise(
              lit("noEvaluable")
            )
          )
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
          .otherwise(lit("noEvaluable"))
      )

  }

}
