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
    *  |-- [column | outputColumnName]: struct (nullable = false)
    *  |    |-- [labelName]: string (nullable = true)
    *  |    |-- source: string (nullable = true) << source
    * }}}
    *
    * @param id               name of id column
    * @param column           to be nested as source
    * @param source           used to indicate the source of the identifier, eg. HGNC, Ensembl, Uniprot
    * @param labelName        defaults to "label"
    * @param outputColumnName if output df should not use `column` as name
    * @return dataframe with columns [id, outputColumnName else column ]
    */
  def transformColumnToLabelAndSourceStruct(dataFrame: DataFrame,
                                            id: String,
                                            column: String,
                                            source: String,
                                            labelName: Option[String] = None,
                                            outputColumnName: Option[String] = None): DataFrame = {
    // need to use a temp id in case the labelName is set to id.
    val idTemp = scala.util.Random.alphanumeric.take(10).mkString
    val colTemp = scala.util.Random.alphanumeric.take(10).mkString
    dataFrame
      .select(col(id).as(idTemp), explode(col(column)).as("source"))
      .withColumn(labelName.getOrElse("label"), typedLit(source))
      .transform(nest(_, List(labelName.getOrElse("label"), "source"), colTemp))
      .groupBy(idTemp)
      .agg(collect_set(colTemp).as(outputColumnName.getOrElse(column)))
      .withColumnRenamed(idTemp, id)
  }

  /**
    * Returns dataframe with `column`'s value as nested structure along with a label indicating source of information.
    *
    * {{{
    *   root
    *  |-- id: string (nullable = true)
    *  |-- [column | outputColumnName]: struct (nullable = false)
    *  |    |-- id: string (nullable = true)
    *  |    |-- source: string (nullable = true) << label
    * }}}
    *
    * @param id               name of id column
    * @param column           to be nested as source
    * @param label            used to indicate the source of the identifier, eg. HGNC, Ensembl, Uniprot
    * @param outputColumnName if output df should not use `column` as name
    * @return dataframe with columns [id, outputColumnName else column ]
    */
  def transformColumnToIdAndSourceStruct(
      id: String,
      column: String,
      label: String,
      outputColumnName: Option[String] = None,
  )(dataFrame: DataFrame): DataFrame = {
    transformColumnToLabelAndSourceStruct(dataFrame,
                                          id,
                                          column,
                                          label,
                                          Some("id"),
                                          outputColumnName)
  }
}
