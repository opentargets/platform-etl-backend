package io.opentargets.etl.backend.target

import org.apache.spark.sql.{Column, functions}
import org.apache.spark.sql.functions.{struct, transform, when, size}
import org.apache.spark.sql.types.{ArrayType, StructType}

object TargetUtils {

  /** transform sourceCol as array column into an array of struct with schema ArrayType(schema) repeating inside
    * constant additionalColumns. See TargetUtilsTest example of usage
    * @return cast(ArrayType(schema)) of the nullable transformation
    */
  def transformArrayToStruct(
      sourceCol: Column,
      additionalColumns: List[Column],
      schema: StructType,
      customCondition: Option[Column] = None
  ): Column =
    when(
      customCondition.getOrElse(size(sourceCol) >= 0),
      transform(sourceCol, c => struct(c :: additionalColumns: _*))
    )
      .cast(ArrayType(schema))
}
