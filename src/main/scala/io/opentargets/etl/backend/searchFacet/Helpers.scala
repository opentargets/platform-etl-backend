package io.opentargets.etl.backend.searchFacet

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.apache.spark.sql.functions.{col, collect_set, lit}

object Helpers extends LazyLogging {

  /** Compute simple facet dataset for the given DataFrame, setting the datasourceId to null.
    *
    * @param dataframe
    *   DataFrame to compute facets from.
    * @param labelField
    *   Field to use as label.
    * @param categoryField
    *   Value to use as category.
    * @param entityIdField
    *   Field to use as entity id.
    * @param sparkSession
    *   Implicit SparkSession.
    * @return
    *   Dataset of Facets.
    */
  def computeSimpleFacet(dataframe: DataFrame,
                         labelField: String,
                         categoryField: String,
                         entityIdField: String
  )(implicit sparkSession: SparkSession): Dataset[Facets] = {
    import sparkSession.implicits._

    val facets: Dataset[Facets] = dataframe
      .select(
        col(labelField).as("label"),
        lit(categoryField).as("category"),
        col(entityIdField).as("id")
      )
      .groupBy("label", "category")
      .agg(collect_set("id").as("entityIds"))
      .withColumn("datasourceId", lit(null).cast("string"))
      .distinct()
      .as[Facets]
    facets
  }

  def getRelevantDataset[T](dataframe: DataFrame,
                            idField: String,
                            idAlias: String,
                            facetField: String
  )(implicit encoder: Encoder[T]): Dataset[T] =
    dataframe
      .select(col(idField).as(idAlias), col(facetField))
      .where(col(facetField).isNotNull)
      .as[T]
}
