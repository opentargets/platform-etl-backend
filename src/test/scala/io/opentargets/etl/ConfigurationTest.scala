package io.opentargets.etl

import org.scalatest.flatspec.AnyFlatSpecLike
import io.opentargets.etl.backend.Configuration
import io.opentargets.etl.backend.Configuration.{InputInfo, OTConfig}
import org.scalatest.matchers.must.Matchers
import pureconfig.ConfigReader

class ConfigurationTest extends AnyFlatSpecLike with Matchers {
  "Pureconfig" should "successfully load standard configuration without error" in {
    val conf: ConfigReader.Result[OTConfig] = Configuration.config
    assert(conf.isRight)
  }

  "The parsed configuration" should "include all necessary segments for the drug-beta step" in {
    def checkFormatAndPath(ii: InputInfo): Boolean = (ii.path nonEmpty) && (ii.format nonEmpty)
    val conf = Configuration.config.right.get
    val inputs = conf.common.inputs
    val inputsForDrugBetaStep = List(inputs.drugChemblIndication, inputs.drugChemblMechanism, inputs.drugChemblMolecule, inputs.drugChemblTarget)

    assert(inputsForDrugBetaStep.forall(checkFormatAndPath))
  }
}