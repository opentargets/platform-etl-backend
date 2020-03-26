import $file.common
import common._

import org.apache.spark.SparkConf
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.typesafe.config.Config

object TargetHelpers {
  implicit class AggregationHelpers(df: DataFrame)(implicit ss: SparkSession) {
    import Configuration._
    import ss.implicits._

    // Manipulate safety info. Pubmed as long and unspecified_interaction_effects should be null in case of empty array.
    def getSafetyInfo: DataFrame = {

      df.withColumn(
          "safetyTransf",
          struct(
            when(
              size(col("safetyRoot.adverse_effects")) > 0,
              expr(
                "transform(safetyRoot.adverse_effects, sft -> named_struct('inhibition_effects', sft.inhibition_effects, 'unspecified_interaction_effects', if(size(sft.unspecified_interaction_effects) > 0, sft.unspecified_interaction_effects, null), 'organs_systems_affected', sft.organs_systems_affected, 'activation_effects', sft.activation_effects, 'references', transform(sft.references, v -> named_struct('pmid',cast(v.pmid AS LONG),'ref_label', v.ref_label, 'ref_link', v.ref_link))))"
              )
            ).alias("adverse_effects"),
            when(
              size(col("safetyRoot.safety_risk_info")) > 0,
              expr(
                "transform(safetyRoot.safety_risk_info, sft -> named_struct('organs_systems_affected', sft.organs_systems_affected, 'safety_liability', sft.safety_liability, 'references', transform(sft.references, v -> named_struct('pmid',cast(v.pmid AS LONG),'ref_label', v.ref_label, 'ref_link', v.ref_link))))"
              )
            ).alias("safety_risk_info")
          )
        )
        .withColumn(
          "safety",
          when(
            size(col("safetyTransf.adverse_effects")) > 0 and size(
              col("safetyTransf.safety_risk_info")
            ) < 1,
            struct(col("safetyTransf.adverse_effects"), lit(null).alias("safety_risk_info"))
          ).when(
              size(col("safetyTransf.adverse_effects")) < 1 and size(
                col("safetyTransf.safety_risk_info")
              ) > 0,
              struct(lit(null).alias("adverse_effects"), col("safetyTransf.safety_risk_info"))
            )
            .when(
              size(col("safetyTransf.adverse_effects")) > 0 and size(
                col("safetyTransf.safety_risk_info")
              ) > 0,
              col("safetyTransf")
            )
            .otherwise(lit(null))
        )
        .drop("safetyTransf", "safetyRoot")

    }

    def setIdAndSelectFromTargets: DataFrame = {
      val selectExpressions = Seq(
        "id",
        "approved_name as approvedName",
        "approved_symbol as approvedSymbol",
        "biotype as bioType",
        "case when (hgnc_id = '') then null else hgnc_id end as hgncId",
        "hallmarks as hallMarks",
        "tractability as tractabilityRoot",
        "safety as safetyRoot",
        "chemicalprobes as chemicalProbes",
        "ortholog",
        "go as goRoot",
        "name_synonyms as nameSynonyms",
        "symbol_synonyms as symbolSynonyms",
        "struct(chromosome, gene_start as start, gene_end as end, strand) as genomicLocation"
      )

      val uniprotStructure =
        """
          |case
          |  when (uniprot_id = '' or uniprot_id = null) then null
          |  else struct(uniprot_id as id,
          |    uniprot_accessions as accessions,
          |    uniprot_function as functions)
          |end as proteinAnnotations
          |""".stripMargin

      val dfTractabilityInfo = df
        .selectExpr(selectExpressions :+ uniprotStructure: _*)
        .withColumn(
          "tractability",
          when(
            size(col("tractabilityRoot.antibody.buckets")) > 0 and size(
              col("tractabilityRoot.smallmolecule.buckets")
            ) < 1,
            struct(col("tractabilityRoot.antibody"), lit(null).alias("smallmolecule"))
          ).when(
              size(col("tractabilityRoot.antibody.buckets")) < 1 and size(
                col("tractabilityRoot.smallmolecule.buckets")
              ) > 0,
              struct(lit(null).alias("antibody"), col("tractabilityRoot.smallmolecule"))
            )
            .when(
              size(col("tractabilityRoot.antibody.buckets")) > 0 and size(
                col("tractabilityRoot.smallmolecule.buckets")
              ) > 0,
              col("tractabilityRoot")
            )
            .otherwise(lit(null))
        )
        .drop("tractabilityRoot")

      val dfGoFixed = dfTractabilityInfo
        .withColumn(
          "go",
          when(
            size(col("goRoot")) > 0,
            expr(
              "transform(goRoot, goEntry -> named_struct('id',goEntry.id, 'value', transform(goRoot, v -> named_struct('evidence', replace(v.value.evidence,':','_'), 'project', v.value.project,'term', v.value.term))))"
            )
          )
        )
        .drop("goRoot")

      // Manipulate safety info. Pubmed as long and unspecified_interaction_effects should be null in case of empty array.
      val dfSafetyInfo = dfGoFixed.getSafetyInfo

      dfSafetyInfo
    }
  }
}

// This is option/step target in the config file
object Target extends LazyLogging {
  def apply(config: Config)(implicit ss: SparkSession) = {
    import ss.implicits._
    import TargetHelpers._

    val common = Configuration.loadCommon(config)
    val mappedInputs = Map(
      "target" -> Map("format" -> common.inputs.target.format, "path" -> common.inputs.target.path)
    )
    val inputDataFrame = SparkSessionWrapper.loader(mappedInputs)

    val targetDF = inputDataFrame("target").setIdAndSelectFromTargets

    SparkSessionWrapper.save(targetDF, common.output + "/targets")

  }
}
