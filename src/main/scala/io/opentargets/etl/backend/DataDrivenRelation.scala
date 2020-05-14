package io.opentargets.etl.backend

import org.apache.spark.SparkConf
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.typesafe.config.Config
import io.opentargets.etl.backend.SparkHelpers.IOResourceConfig

object DataDrivenRelationsHelpers {
  implicit class AggregationHelpers(df: DataFrame)(implicit ss: SparkSession) {
    import Configuration._
    import ss.implicits._

    def getDataDrivenRelationgEntity: Map[String, DataFrame] = {
      val selectExpressions = Seq(
        "id",
        "subject.id as A",
        "object.id as B",
        "scores.overlap as score",
        "counts.shared_count as countAAndB",
        "counts.union_count as countAOrB"
      )

      val selectExpressionsTarget = Seq(
        "subject.links.diseases_count as countA",
        "object.links.diseases_count as countB",
        "shared_diseases as relatedInfo"
      )

      val selectExpressionsDisease = Seq(
        "subject.links.targets_count as countA",
        "object.links.targets_count as countB",
        "shared_targets as relatedInfo"
      )

      val dataDrivenRelationOutputs = Map(
        "targetRelation" -> df
          .filter(col("type") === "shared-disease")
          .selectExpr(selectExpressions ++ selectExpressionsTarget: _*),
        "diseaseRelation" -> df
          .filter(col("type") === "shared-target")
          .selectExpr(selectExpressions ++ selectExpressionsDisease: _*)
      )
      dataDrivenRelationOutputs
    }
  }
}

// This is option/step DataDrivenRelation in the config file
object DataDrivenRelation extends LazyLogging {
  def apply()(implicit context: ETLSessionContext) = {
    implicit val ss = context.sparkSession
    import ss.implicits._
    import DataDrivenRelationsHelpers._

    val common = context.configuration.common
    val mappedInputs = Map(
      "ddr" -> IOResourceConfig(common.inputs.ddr.format, common.inputs.ddr.path)
    )
    val inputDataFrame = SparkHelpers.readFrom(mappedInputs)

    val dfOutputs = inputDataFrame("ddr").getDataDrivenRelationgEntity

    // TODO THIS NEEDS MORE REFACTORING WORK AS IT CAN BE SIMPLIFIED
    val outputs = dfOutputs.map {
      case (dfName, df) =>
        val name = IOResourceConfig(context.configuration.common.outputFormat,
                                    context.configuration.common.output + s"/$dfName")
        (dfName -> name, dfName -> df)
    }
    val unzipped = outputs.unzip
    SparkHelpers.writeTo(unzipped._1.toMap, unzipped._2.toMap)
  }
}
