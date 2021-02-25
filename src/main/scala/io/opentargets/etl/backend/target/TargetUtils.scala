package io.opentargets.etl.backend.target

import io.opentargets.etl.backend.spark.Helpers.nest
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, collect_set, explode, typedLit}

case class LabelAndSource(label: String, source: String)

case class LocationAndSource(location: String, source: String)

object TargetUtils {

  /**
    * Returns dataframe with `column`'s value as nested structure along with a label indicating source of information.
    *
    * {{{
    *   root
    *  |-- id: string (nullable = true)
    *  |-- [column]: struct (nullable = false)
    *  |    |-- [labelName]: string (nullable = true)
    *  |    |-- source: string (nullable = true)
    * }}}
    *
    * @param id               name of id column
    * @param column           to be nested as source
    * @param label            used to indicate the source of the identifier, eg. HGNC, Ensembl, Uniprot
    * @param labelName        defaults to "label"
    * @param outputColumnName if output df should not use `column` as name
    * @return dataframe with columns [id, column | newname ]
    */
  def transformColumnToLabelAndSourceStruct(dataFrame: DataFrame,
                                            id: String,
                                            column: String,
                                            label: String,
                                            labelName: Option[String] = None,
                                            outputColumnName: Option[String] = None): DataFrame = {
    // need to use a temp id in case the labelName is set to id.
    val idTemp = scala.util.Random.alphanumeric.take(10).mkString
    dataFrame
      .select(col(id).as(idTemp), explode(col(column)).as("source"))
      .withColumn(labelName.getOrElse("label"), typedLit(label))
      .transform(nest(_, List(labelName.getOrElse("label"), "source"), column))
      .groupBy(idTemp)
      .agg(collect_set(column).as(outputColumnName.getOrElse(column)))
      .withColumnRenamed(idTemp, id)
  }
}
