package io.opentargets.etl.backend

import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.must.Matchers

trait EtlSparkUnitTest
  extends AnyFlatSpecLike
    with PrivateMethodTester
    with SparkSessionSetup
    with Matchers
