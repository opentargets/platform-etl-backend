package io.opentargets.etl

import com.typesafe.scalalogging.LazyLogging

sealed trait ETLComponent
trait ETLStep extends ETLComponent {
  val name: String
  val description: Option[String] = None
  val dependencies: List[ETLStep] = List.empty
  def hasDependencies: Boolean = dependencies.nonEmpty
}
case class ETLMeta(name: String) extends ETLStep
case object ETLEmpty extends ETLStep {
  override val name: String = "ETLEmpty"
}

case class ETLAction(name: String)

object ETLPipeline extends ETLComponent with LazyLogging {
  def apply: ETLComponent = {

    ETLEmpty
  }
}
