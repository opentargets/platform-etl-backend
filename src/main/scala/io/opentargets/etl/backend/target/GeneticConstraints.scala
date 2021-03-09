package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.{array, col, lit, struct}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class GeneticConstraintElement(constraintType: String,
                                    score: String,
                                    exp: String,
                                    oe: String,
                                    oeLower: String,
                                    oeUpper: String)

case class GeneticConstraint(id: String, constraint: Seq[GeneticConstraintElement])

object GeneticConstraints extends LazyLogging {

  def apply(df: DataFrame)(implicit sparkSession: SparkSession): Dataset[GeneticConstraint] = {
    logger.info("Calculating genetic constraints")
    import sparkSession.implicits._
    implicit def myDataEncoder: org.apache.spark.sql.Encoder[GeneticConstraintElement] =
      org.apache.spark.sql.Encoders.kryo[GeneticConstraintElement]

    df.select(
        col("gene_id").as("id"),
        array(
          struct(
            lit("syn").as("constraintType"),
            col("syn_z").as("score"),
            col("exp_syn").as("exp"),
            col("obs_syn").as("obs"),
            col("oe_syn").as("oe"),
            col("oe_syn_lower").as("oeLower"),
            col("oe_syn_upper").as("oeUpper")
          ),
          struct(
            lit("mis").as("constraintType"),
            col("mis_z").as("score"),
            col("exp_mis").as("exp"),
            col("obs_mis").as("obs"),
            col("oe_mis").as("oe"),
            col("oe_mis_lower").as("oeLower"),
            col("oe_mis_upper").as("oeUpper")
          ),
          struct(
            lit("lof").as("constraintType"),
            col("pLi").as("score"),
            col("exp_lof").as("exp"),
            col("obs_lof").as("obs"),
            col("oe_lof").as("oe"),
            col("oe_lof_lower").as("oeLower"),
            col("oe_lof_upper").as("oeUpper")
          )
        ).as("constraint")
      )
      .as[GeneticConstraint]
  }
}
