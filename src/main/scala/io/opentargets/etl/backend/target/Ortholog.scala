package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}

object Ortholog extends LazyLogging {

  def apply(orthologs: DataFrame, targetSpecies: List[String])(implicit ss: SparkSession): DataFrame = {
    ???

  }

}
