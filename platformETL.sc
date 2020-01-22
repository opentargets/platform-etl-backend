import $file.backend.common
import common._
import $file.backend.disease
import disease._
import $file.backend.target
import target._
import $file.backend.drug
import drug._
import $ivy.`com.typesafe:config:1.3.1`
import scala.collection.mutable.HashMap

/**
  Read by default the conf file amm.application.conf and it generates all the indexes.
  step: disease, target, drug
*/
@main
def main(
    step: String = "all",
    conf: String = "resources/amm.application.conf"
): Unit = {

  import DiseaseTransformers.ImplicitsDisease
  import TargetTransformers.ImplicitsTarget
  import DrugTransformers.ImplicitsDrug

  val cfg = getConfig(conf)
  val listInputFiles = getInputFiles(cfg, step)
  val outputPathPrefix = cfg.getString("output-dir")
  
  
  val spark = SparkSessionWrapper.spark
  val inputDataFrame = SparkSessionWrapper.loader(listInputFiles)
 
  step match {
    case "disease" => SparkSessionWrapper.save(inputDataFrame("disease").diseaseIndex, outputPathPrefix + "/diseases")
    case "target"  => SparkSessionWrapper.save(inputDataFrame("target").targetIndex, outputPathPrefix + "/targets")
    case "drug"    => SparkSessionWrapper.save(inputDataFrame("drug").drugIndex(inputDataFrame("evidence")), outputPathPrefix + "/drugs")
    case "all" => {
      SparkSessionWrapper.save(inputDataFrame("disease").diseaseIndex, outputPathPrefix + "/diseases")
      SparkSessionWrapper.save(inputDataFrame("target").targetIndex, outputPathPrefix + "/targets")
	  SparkSessionWrapper.save(inputDataFrame("drug").drugIndex(inputDataFrame("evidence")), outputPathPrefix + "/drugs")
    }
    case _ => {
	  // TODO
      println("Exit with error or ALL by defaul (?) ")
    }
  }

  //if (inputDataFrame.contains("disease")) println("Size diseases:" + inputDataFrame("disease").count())
  //if (inputDataFrame.contains("target")) println("Size target:" + inputDataFrame("target").count())

}
