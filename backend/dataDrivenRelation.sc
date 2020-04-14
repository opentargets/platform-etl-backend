import $file.common
import common._

import org.apache.spark.SparkConf
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.typesafe.config.Config

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
  def apply(config: Config)(implicit ss: SparkSession) = {
    import ss.implicits._
    import DataDrivenRelationsHelpers._

    val common = Configuration.loadCommon(config)
    val mappedInputs = Map(
      "ddr" -> Map("format" -> common.inputs.ddr.format, "path" -> common.inputs.ddr.path)
    )
    val inputDataFrame = SparkSessionWrapper.loader(mappedInputs)

    val dfOutputs = inputDataFrame("ddr").getDataDrivenRelationgEntity

    dfOutputs.keys.foreach { index =>
      SparkSessionWrapper.save(dfOutputs(index), common.output + "/" + index)
    }

  }
}
