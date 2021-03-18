package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers.nest
import org.apache.spark.sql.functions.{array, col, lit, struct}
import org.apache.spark.sql.types.{FloatType, IntegerType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class GeneticConstraintElement(constraintType: String,
                                    score: String,
                                    exp: String,
                                    oe: String,
                                    oeLower: String,
                                    oeUpper: String)

/**
  *
  * @param id          Ensembl gene id
  * @param constraints Array of [mis, lof, syn]
  * @param upperRank   Loss of function: It’s telling you what’s the relative genetic constraint of this gene as compared
  *                    to all the other genes. For example  “upperRank” = 1 would be the gene with the highest oeUpper
  *                    value.
  * @param upperBin
  * @param upperBin6
  */
case class GeneticConstraint(constraints: Array[GeneticConstraintElement],
                             upperRank: Int,
                             upperBin: Int,
                             upperBin6: Int)

case class GeneticConstraintsWithId(
    id: String,
    geneticConstraint: GeneticConstraint
)

object GeneticConstraints extends LazyLogging {

  def apply(df: DataFrame)(
      implicit sparkSession: SparkSession): Dataset[GeneticConstraintsWithId] = {
    logger.info("Calculating genetic constraints")
    import sparkSession.implicits._

    df.select(
        col("gene_id").as("id"),
        array(
          struct(
            lit("syn").as("constraintType"),
            col("syn_z").cast(FloatType).as("score"),
            col("exp_syn").cast(FloatType).as("exp"),
            col("obs_syn").cast(IntegerType).as("obs"),
            col("oe_syn").cast(FloatType).as("oe"),
            col("oe_syn_lower").cast(FloatType).as("oeLower"),
            col("oe_syn_upper").cast(FloatType).as("oeUpper")
          ),
          struct(
            lit("mis").as("constraintType"),
            col("mis_z").cast(FloatType).as("score"),
            col("exp_mis").cast(FloatType).as("exp"),
            col("obs_mis").cast(IntegerType).as("obs"),
            col("oe_mis").cast(FloatType).as("oe"),
            col("oe_mis_lower").cast(FloatType).as("oeLower"),
            col("oe_mis_upper").cast(FloatType).as("oeUpper")
          ),
          struct(
            lit("lof").as("constraintType"),
            col("pLi").cast(FloatType).as("score"),
            col("exp_lof").cast(FloatType).as("exp"),
            col("obs_lof").cast(IntegerType).as("obs"),
            col("oe_lof").cast(FloatType).as("oe"),
            col("oe_lof_lower").cast(FloatType).as("oeLower"),
            col("oe_lof_upper").cast(FloatType).as("oeUpper")
          )
        ).as("constraints"),
        col("oe_lof_upper_rank").cast(IntegerType).as("upperRank"),
        col("oe_lof_upper_bin").cast(IntegerType).as("upperBin"),
        col("oe_lof_upper_bin_6").cast(IntegerType).as("upperBin6")
      )
      .transform(
        nest(_, List("constraints", "upperRank", "upperBin", "upperBin6"), "geneticConstraint"))
      .as[GeneticConstraintsWithId]
  }
}
