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
    johnS,
    breeze
  ).flatten

  lazy val cli = Seq(
    "com.monovore" %% "decline-effect" % "2.5.0"
  )

  lazy val betterFiles = Seq("com.github.pathikrit" %% "better-files-akka" % "3.9.2")

  lazy val configDeps = Seq(
    "com.github.pureconfig" %% "pureconfig" % "0.17.8"
  )

  lazy val loggingDeps = Seq(
    "ch.qos.logback" % "logback-classic" % "1.5.18",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5"
  )

  lazy val graphDeps = Seq(
    "org.jgrapht" % "jgrapht-core" % "1.5.2"
  )

  lazy val sparkVersion = "3.2.4"

  lazy val breeze = Seq(
    "org.scalanlp" %% "breeze" % "2.1.0"
  )

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
      "com.databricks" %% "spark-xml" % "0.18.0"
    ) ++ sparkDeps
  }

  lazy val testVersion = "3.2.19"
  lazy val testingDeps = Seq(
    "org.scalactic" %% "scalactic" % testVersion % Test,
    "org.scalatest" %% "scalatest" % testVersion % Test,
    "org.scalamock" %% "scalamock" % "6.2.0" % Test
  )

  lazy val typeSafeConfig = Seq("com.typesafe" % "config" % "1.4.3")

  lazy val gcpEtl = Seq(
    "com.google.cloud" % "google-cloud-dataproc" % "4.57.0" % "provided",
    "com.google.cloud" % "google-cloud-storage" % "2.50.0"
  )

  lazy val johnSVersion = "5.5.3"
  lazy val johnS = Seq(
    "com.johnsnowlabs.nlp" % "spark-nlp_2.12" % johnSVersion
  )
}
