package io.opentargets.etl.backend.literature

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.spark.IOResource
import io.opentargets.etl.backend.spark.IoHelpers.writeTo
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.functions._
import org.apache.spark.ml.linalg.Vectors._
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.functions._

object Vectors extends Serializable with LazyLogging {
  val columns = List("category", "word", "norm", "vector")

  private def loadVectorsFromModel(path: String): DataFrame = {
    logger.info("load vectors from model")
    Word2VecModel.load(path).getVectors
  }

  def compute(vectors: DataFrame)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    val mdf = vectors
    logger.info(s"transform vectors dataframe adding missing columns")

    mdf
      .withColumn("category",
                  when($"word".startsWith("ENSG"), lit("target"))
                    .when($"word".startsWith("CHEMBL"), lit("drug"))
                    .otherwise("disease")
      )
      .withColumn("norm", udf((v: Vector) => norm(v, 2d)).apply($"vector"))
      .withColumn("vector", vector_to_array($"vector"))
      .select(columns.head, columns.tail: _*)
  }

  def apply()(implicit context: ETLSessionContext): Unit = {
    implicit val ss: SparkSession = context.sparkSession

    logger.info("Generate vector table from W2V model")
    val configuration = context.configuration.literature.vectors

    val mdf = loadVectorsFromModel(configuration.input)
    val vdf = compute(mdf)

    val dataframesToSave = Map(
      "vectorsIndex" -> IOResource(vdf, configuration.output)
    )

    writeTo(dataframesToSave)
  }
}
