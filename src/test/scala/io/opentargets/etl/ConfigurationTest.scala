package io.opentargets.etl

import org.scalatest.flatspec.AnyFlatSpecLike
import io.opentargets.etl.backend.Configuration
import io.opentargets.etl.backend.Configuration.{InputInfo, OTConfig}
import org.scalatest.matchers.must.Matchers
import pureconfig.ConfigReader

class ConfigurationTest extends AnyFlatSpecLike with Matchers {
  "Pureconfig" should "successfully load standard configuration without error" in {
    val conf: ConfigReader.Result[OTConfig] = Configuration.config
    assert(conf.isRight, s"Failed with ${conf.left}")
  }

  "The parsed configuration" should "include all necessary segments for the drug-beta step" in {
    def checkFormatAndPath(ii: InputInfo): Boolean = (ii.path nonEmpty) && (ii.format nonEmpty)
    val conf = Configuration.config.right.get
    val inputs = conf.common.inputs.drugBeta
    val inputsForDrugBetaStep = List(
      inputs.chemblIndication,
      inputs.chemblMechanism,
      inputs.chemblMolecule,
      inputs.chemblTarget,
      inputs.drugbankToChembl,
      inputs.diseasePipeline,
      inputs.targetPipeline,
      inputs.evidencePipeline
    )

    assert(inputsForDrugBetaStep.forall(checkFormatAndPath))
  }
}
