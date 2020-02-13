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
      val selectExpressions = (
        Seq(
          "id",
          "type as typeRelation",
          "subject.id as A",
          "object.id as B",
          "scores.overlap as score",
          "subject.links.targets_count as targetCountA",
          "object.links.targets_count as targetCountB",
          "subject.links.diseases_count as diseaseCountA",
          "object.links.diseases_count as diseaseCountB",
          "counts.shared_count as countAndB",
          "counts.union_count as countAOrB",
          "shared_diseases as shared_diseases",
          "shared_targets as shared_targets"
        )
      )

      val ddrDf = df.selectExpr(selectExpressions: _*)

      val dataDrivenRelationOutputs = Map(
        "targetRelation" -> ddrDf
          .filter(col("typeRelation") === "shared-disease")
          .drop("shared_targets", "targetCountA", "targetCountB", "typeRelation"),
        "diseaseRelation" -> ddrDf
          .filter(col("typeRelation") === "shared-target")
          .drop("shared_diseases", "diseaseCountA", "diseaseCountB", "typeRelation")
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
    val mappedInputs = Map("ddr" -> common.inputs.ddr)
    val inputDataFrame = SparkSessionWrapper.loader(mappedInputs)

    val dfOutputs = inputDataFrame("ddr").getDataDrivenRelationgEntity

    dfOutputs.keys.foreach { index =>
      SparkSessionWrapper.save(dfOutputs(index), common.output + "/" + index)
    }

  }
}
