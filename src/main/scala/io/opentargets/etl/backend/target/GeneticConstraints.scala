package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, Dataset}

case class GeneticConstraint()

object GeneticConstraints extends LazyLogging {

  def apply(df: DataFrame): Dataset[GeneticConstraint] = {
    logger.info("Calculating genetic constraints")
    ???
  }
}
