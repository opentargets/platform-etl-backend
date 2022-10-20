import sbt._

object Dependencies {

  lazy val workflowDependencies: Seq[ModuleID] = Seq(
    configDeps,
    cats,
    gcp,
    testingDeps,
    loggingDeps
  ).flatten

  lazy val etlDependencies: Seq[ModuleID] = Seq(
    betterFiles,
    configDeps,
    loggingDeps,
    graphDeps,
    sparkDeps,
    testingDeps,
    gcp,
    typeSafeConfig
  ).flatten

  lazy val catsVersion = "2.5.3"
  lazy val cats = Seq(
    "org.typelevel" %% "cats-effect" % catsVersion,
    "org.typelevel" %% "log4cats-slf4j" % "2.5.0",
    "com.github.pureconfig" %% "pureconfig-cats-effect" % "0.17.1",
    "org.typelevel" %% "cats-effect-testing-scalatest" % "1.4.0" % Test
  )

  lazy val betterFiles = Seq("com.github.pathikrit" %% "better-files-akka" % "3.9.1")

  lazy val configDeps = Seq(
    "com.github.pureconfig" %% "pureconfig" % "0.14.1"
  )

  lazy val loggingDeps = Seq(
    "ch.qos.logback" % "logback-classic" % "1.4.1",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
  )

  lazy val graphDeps = Seq(
    "org.jgrapht" % "jgrapht-core" % "1.4.0"
  )

  lazy val sparkVersion = "3.1.3"
  lazy val sparkDeps = Seq(
    "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly (),
    "com.databricks" %% "spark-xml" % "0.11.0",
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-graphx" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"
  )

  lazy val testVersion = "3.2.2"
  lazy val testingDeps = Seq(
    "org.scalactic" %% "scalactic" % testVersion,
    "org.scalatest" %% "scalatest" % testVersion % "test",
    "org.scalamock" %% "scalamock" % "5.1.0" % "test"
  )

  lazy val typeSafeConfig = Seq("com.typesafe" % "config" % "1.4.1")

  lazy val gcp = Seq(
    "com.google.cloud" % "google-cloud-dataproc" % "2.3.2" % "provided",
    "com.google.cloud" % "google-cloud-storage" % "2.4.2"
  )
}
