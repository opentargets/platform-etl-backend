package io.opentargets.etl.backend

import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import io.opentargets.etl.backend.spark.Helpers._

class HelpersTest extends AnyFlatSpecLike with Matchers {
  // given
  val renameFun: String => String = _.toUpperCase
  val testStruct =
    StructType(
      StructField("a", IntegerType, nullable = true) ::
        StructField("b", LongType, nullable = false) ::
        StructField("c", BooleanType, nullable = false) :: Nil)

  "Rename columns" should "rename all columns using given function" in {

    // when
    val results: StructType = renameAllCols(testStruct, renameFun)
    // then
    assert(results.fields.forall(sf => sf.name.head.isUpper))
  }

  it should "correctly rename columns in nested arrays" in {
    // given
    val structWithArray = testStruct
      .add("d",ArrayType(new StructType()
        .add("e",StringType)
        .add("f",StringType)
        .add("g",IntegerType)))
    // when
    val results = renameAllCols(structWithArray, renameFun)
    // then
    assert(results(3).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType].fieldNames.forall(_.head.isUpper))
  }
}
