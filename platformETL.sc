import $file.backend.common
import common._
import $file.backend.disease
import disease._
import $file.backend.target
import target._
import $file.backend.drug
import drug._
import $file.backend.associations
import associations._
import $file.backend.search
import search._
import $ivy.`com.typesafe:config:1.4.0`
import $ivy.`ch.qos.logback:logback-classic:1.2.3`
import $ivy.`com.typesafe.scala-logging::scala-logging:3.9.2`
import $ivy.`com.typesafe:config:1.4.0`

import com.typesafe.scalalogging.LazyLogging

object ETL extends LazyLogging {
  def apply(step: String) = {
    import DiseaseTransformers.ImplicitsDisease
    import TargetTransformers.ImplicitsTarget
    import DrugTransformers.ImplicitsDrug

    val otc = Configuration.load

    //  val cfg = getConfig(conf)
    //  val listInputFiles = getInputFiles(cfg, step)
    //  val outputPathPrefix = cfg.getString("output-dir")

    implicit val spark = SparkSessionWrapper.session
    //  val inputDataFrame = SparkSessionWrapper.loader(listInputFiles)

    step match {
      case "search" =>
        logger.info("run step search")
        Search(otc)
      case "associations" =>
        logger.info("run step associations")
        Associations(otc)

      //    case "disease" => SparkSessionWrapper.save(inputDataFrame("disease").diseaseIndex, outputPathPrefix + "/diseases")
      //    case "target"  => SparkSessionWrapper.save(inputDataFrame("target").targetIndex, outputPathPrefix + "/targets")
      //    case "drug"    => SparkSessionWrapper.save(inputDataFrame("drug").drugIndex(inputDataFrame("evidence")), outputPathPrefix + "/drugs")
      //    case "all" => {
      //      SparkSessionWrapper.save(inputDataFrame("disease").diseaseIndex, outputPathPrefix + "/diseases")
      //      SparkSessionWrapper.save(inputDataFrame("target").targetIndex, outputPathPrefix + "/targets")
      //	  SparkSessionWrapper.save(inputDataFrame("drug").drugIndex(inputDataFrame("evidence")), outputPathPrefix + "/drugs")
      //    }
      case _ =>
        logger.error("Exit with error or ALL by defaul (?) ")
    }

  }
}
/**
  Read by default the conf file amm.application.conf and it generates all the indexes.
  step: disease, target, drug
*/
@main
def main(
    step: String = "all",
): Unit = {
  ETL(step)
}
