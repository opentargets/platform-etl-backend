import sbt._

import scala.language.postfixOps

object Dependencies {

  lazy val etlDependencies: Seq[ModuleID] = Seq(
    betterFiles,
    configDeps,
    loggingDeps,
    graphDeps,
    sparkDeps,
    testingDeps,
    gcpEtl,
    typeSafeConfig,
    johnS
  ).flatten

  lazy val cli = Seq(
    "com.monovore" %% "decline-effect" % "2.3.1"
  )

  lazy val betterFiles = Seq("com.github.pathikrit" %% "better-files-akka" % "3.9.1")

  lazy val configDeps = Seq(
    "com.github.pureconfig" %% "pureconfig" % "0.17.1"
  )

  lazy val loggingDeps = Seq(
    "ch.qos.logback" % "logback-classic" % "1.4.4",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
  )

  lazy val graphDeps = Seq(
    "org.jgrapht" % "jgrapht-core" % "1.4.0"
  )

  lazy val sparkVersion = "3.2.4"

  lazy val sparkDeps: Seq[ModuleID] = {
    val sparkDepsOptionallyProvided = Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-graphx" % sparkVersion,
      "org.apache.spark" %% "spark-mllib" % sparkVersion
    )
    val sparkDeps =
      if (sys.props.getOrElse("ETL_FLAG_DATAPROC", "true").toBoolean) {
        sparkDepsOptionallyProvided.map(d => d % "provided")
      } else {
        sparkDepsOptionallyProvided
      }
    Seq(
      "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly (),
      "com.databricks" %% "spark-xml" % "0.11.0"
    ) ++ sparkDeps
  }

  lazy val testVersion = "3.2.2"
  lazy val testingDeps = Seq(
    "org.scalactic" %% "scalactic" % testVersion % Test,
    "org.scalatest" %% "scalatest" % testVersion % Test,
    "org.scalamock" %% "scalamock" % "5.1.0" % Test
  )

  lazy val typeSafeConfig = Seq("com.typesafe" % "config" % "1.4.1")

  lazy val gcpEtl = Seq(
    "com.google.cloud" % "google-cloud-dataproc" % "4.2.0" % "provided",
    "com.google.cloud" % "google-cloud-storage" % "2.4.2"
  )

  lazy val johnSVersion = "3.3.4"
  lazy val johnS = Seq(
    "com.johnsnowlabs.nlp" % "spark-nlp_2.12" % johnSVersion
  )
}
