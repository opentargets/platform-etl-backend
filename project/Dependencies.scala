import sbt._

object Dependencies {

  lazy val workflowDependencies: Seq[ModuleID] = Seq(
    configDeps,
    cats,
    gcpWorkflow,
    testingDeps,
    loggingDeps,
    cli
  ).flatten

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

  lazy val catsVersion = "3.3.14"
  lazy val cats = Seq(
    "org.typelevel" %% "cats-effect" % catsVersion,
    "org.typelevel" %% "log4cats-slf4j" % "2.5.0",
    "com.github.pureconfig" %% "pureconfig-cats-effect" % "0.17.1",
    "org.typelevel" %% "cats-effect-testing-scalatest" % "1.4.0" % Test
  )
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
    "org.scalactic" %% "scalactic" % testVersion % Test,
    "org.scalatest" %% "scalatest" % testVersion % Test,
    "org.scalamock" %% "scalamock" % "5.1.0" % Test
  )

  lazy val typeSafeConfig = Seq("com.typesafe" % "config" % "1.4.1")

  lazy val gcpEtl = Seq(
    "com.google.cloud" % "google-cloud-dataproc" % "4.2.0" % "provided",
    "com.google.cloud" % "google-cloud-storage" % "2.4.2"
  )
  lazy val gcpWorkflow = Seq(
    "com.google.cloud" % "google-cloud-dataproc" % "4.2.0",
    "com.google.cloud" % "google-cloud-storage" % "2.4.2",
    // https://storage.googleapis.com/cloud-opensource-java-dashboard/com.google.cloud/libraries-bom/26.1.4/index.html
    "io.grpc" % "grpc-netty-shaded" % "1.50.2", // needed for workflow to communicate with GCP
    "io.grpc" % "grpc-netty" % "1.50.2", // needed for workflow to communicate with GCP
    "io.grpc" % "grpc-okhttp" % "1.50.2" // needed for workflow to communicate with GCP
  )

  lazy val johnSVersion = "3.3.4"
  lazy val johnS = Seq(
    "com.johnsnowlabs.nlp" % "spark-nlp_2.12" % johnSVersion
  )
}
