package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{array, col, lit, ntile, struct, when}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/** @param constraintType
  *   One of [mis, lof, syn]
  * @param upperRank
  *   Loss of function: It’s telling you what’s the relative genetic constraint of this gene as
  *   compared to all the other genes. For example “upperRank” = 1 would be the gene with the
  *   highest oeUpper value. null if constraintType is not lof
  * @param upperBin
  *   null if constraintType is not lof
  * @param upperBin6
  *   null if constraintType is not lof
  */
case class GeneticConstraint(
    constraintType: String,
    score: String,
    exp: String,
    oe: String,
    oeLower: String,
    oeUpper: String,
    upperRank: Int,
    upperBin: Int,
    upperBin6: Int
)

case class GeneticConstraintsWithId(
    id: String,
    constraint: Array[GeneticConstraint]
)

object GeneticConstraints extends LazyLogging {

  def apply(
      df: DataFrame
  )(implicit sparkSession: SparkSession): Dataset[GeneticConstraintsWithId] = {
    logger.info("Calculating genetic constraints")
    import sparkSession.implicits._

    df.withColumn(
      "lof.oe_ci.upper_bin_sextile",
        when(col("lof.oe_ci.upper_rank") =!= "NA",
          ntile(6).over(Window.orderBy(col("lof.oe_ci.upper_rank").cast(IntegerType))) - 1
        ).otherwise("NA")
      )
      .select(
      col("gene_id").cast(StringType).as("id"),
      array(
        struct(
          lit("syn").as("constraintType"),
          col("syn.z_score").cast(FloatType).as("score"),
          col("syn.exp").cast(FloatType).as("exp"),
          col("syn.obs").cast(IntegerType).as("obs"),
          col("syn.oe").cast(FloatType).as("oe"),
          col("syn.oe_ci.lower").cast(FloatType).as("oeLower"),
          col("syn.oe_ci.upper").cast(FloatType).as("oeUpper"),
          lit(null).as("upperRank"),
          lit(null).as("upperBin"),
          lit(null).as("upperBin6")
        ),
        struct(
          lit("mis").as("constraintType"),
          col("mis.z_score").cast(FloatType).as("score"),
          col("mis.exp").cast(FloatType).as("exp"),
          col("mis.obs").cast(IntegerType).as("obs"),
          col("mis.oe").cast(FloatType).as("oe"),
          col("mis.oe_ci.lower").cast(FloatType).as("oeLower"),
          col("mis.oe_ci.upper").cast(FloatType).as("oeUpper"),
          lit(null).as("upperRank"),
          lit(null).as("upperBin"),
          lit(null).as("upperBin6")
        ),
        struct(
          lit("lof").as("constraintType"),
          col("lof.pLI").cast(FloatType).as("score"),
          col("lof.exp").cast(FloatType).as("exp"),
          col("lof.obs").cast(IntegerType).as("obs"),
          col("lof.oe").cast(FloatType).as("oe"),
          col("lof.oe_ci.lower").cast(FloatType).as("oeLower"),
          col("lof.oe_ci.upper").cast(FloatType).as("oeUpper"),
          col("lof.oe_ci.upper_rank").cast(IntegerType).as("upperRank"),
          col("lof.oe_ci.upper_bin_decile").cast(IntegerType).as("upperBin"),
          col("lof.oe_ci.upper_bin_sextile").cast(IntegerType).as("upperBin6")
        )
      ).as("constraint")
    ).as[GeneticConstraintsWithId]
  }
}
