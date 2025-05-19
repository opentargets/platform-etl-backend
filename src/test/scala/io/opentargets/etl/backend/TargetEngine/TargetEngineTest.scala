package io.opentargets.etl.backend.TargetEngine

import io.opentargets.etl.backend.{ETLSessionContext, EtlSparkUnitTest}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType, DoubleType}
import io.opentargets.etl.backend.targetEngine.Functions.mousemodQuery

import scala.collection.JavaConverters._

object TargetEngineTest {
  def createQuerySetDf(implicit context: ETLSessionContext): DataFrame = {
    val rows = Seq(Row("ENSG00000083454"), Row("ENSG00000092201"), Row("ENSG00000105707"))
    val schema = StructType(StructField("targetId", StringType, nullable = false) :: Nil)
    context.sparkSession.createDataFrame(context.sparkSession.sparkContext.parallelize(rows),
                                         schema
    )
  }

  def createMouseDf(implicit context: ETLSessionContext): DataFrame = {
    val rows = Seq(
      Row(
        "ENSG00000105707",
        Seq(
          Row("MP:0005377", "hearing/vestibular/ear phenotype"),
          Row("MP:0003631", "nervous system phenotype")
        )
      ),
      Row(
        "ENSG00000092201",
        Seq(
          Row("MP:0005386", "behavior/neurological phenotype")
        )
      ),
      Row(
        "ENSG00000083454",
        Seq(
          Row("MP:0005397", "hematopoietic system phenotype"),
          Row("MP:0005387", "immune system phenotype")
        )
      )
    )
    val modelPhenotypeClassSchema = StructType(
      StructField("id", StringType, nullable = false) :: StructField("label",
                                                                     StringType,
                                                                     nullable = false
      ) :: Nil
    )
    val schema = StructType(
      StructField("targetFromSourceId", StringType, nullable = false) ::
        StructField("modelPhenotypeClasses", ArrayType(modelPhenotypeClassSchema)) ::
        Nil
    )
    context.sparkSession.createDataFrame(context.sparkSession.sparkContext.parallelize(rows),
                                         schema
    )
  }
  def createMousePhenoScoreDf(implicit context: ETLSessionContext): DataFrame = {
    val rows = Seq(Row("MP:0003631", 0.75),
                   Row("MP:0005386", 0.75),
                   Row("MP:0005377", 0.5),
                   Row("MP:0005397", 0.5),
                   Row("MP:0005387", 0.5)
    )
    val schema = StructType(
      StructField("id", StringType, nullable = false) :: StructField("score",
                                                                     DoubleType,
                                                                     nullable = false
      ) :: Nil
    )
    context.sparkSession.createDataFrame(context.sparkSession.sparkContext.parallelize(rows),
                                         schema
    )
  }
}

class TargetEngineTest extends EtlSparkUnitTest {

  "Function mousemodQuery" should "return  dataframe" in {

    // given
    val querySetDf: DataFrame = TargetEngineTest.createQuerySetDf(ctx)
    val mouseDf: DataFrame = TargetEngineTest.createMouseDf(ctx)
    val mousePhenoScoresDf: DataFrame = TargetEngineTest.createMousePhenoScoreDf(ctx)
    val expectedColumns = Set("targetId",
                              "target_id_",
                              "score",
                              "harmonicSum",
                              "maxHarmonicSum",
                              "maximum",
                              "scaledHarmonicSum",
                              "negScaledHarmonicSum"
    )

    // when
    val results: DataFrame = mousemodQuery(querySetDf, mouseDf, mousePhenoScoresDf)

    // then
    assert(expectedColumns.forall(expectedCol => results.columns.contains(expectedCol)))

  }
}
