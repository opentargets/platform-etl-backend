import sbt._

object Dependencies {

  lazy val dependencies: Seq[ModuleID] = Seq(
    aoyi,
    Seq(betterFiles),
    codeDeps,
    configDeps,
    loggingDeps,
    graphDeps,
    sparkDeps,
    testingDeps,
    gcp,
    Seq(typeSafeConfig),
    cats,
    monocle,
    smile,
    johnS
  ).flatten

  lazy val aoyi = Seq(
    "com.lihaoyi" %% "pprint" % "0.6.0"
  )

  lazy val betterFiles = "com.github.pathikrit" %% "better-files-akka" % "3.9.1"

  lazy val codeDeps = Seq(
    "com.beachape" %% "enumeratum" % "1.6.1",
    "com.github.scopt" %% "scopt" % "3.7.1"
  )

  lazy val configDeps = Seq(
    "com.github.pureconfig" %% "pureconfig" % "0.14.1"
  )

  lazy val loggingDeps = Seq(
    "ch.qos.logback" % "logback-classic" % "1.2.3",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
  )

  lazy val graphDeps = Seq(
    "org.jgrapht" % "jgrapht-core" % "1.4.0"
  )

  lazy val sparkVersion = "3.1.1"
  lazy val sparkDeps = Seq(
    "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly (),
    "com.databricks" %% "spark-xml" % "0.11.0",
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-graphx" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"
  )
  lazy val scalaCheck = "org.scalacheck" %% "scalacheck" % "1.14.3"

  lazy val testVersion = "3.2.2"
  lazy val testingDeps = Seq(
    "org.scalactic" %% "scalactic" % testVersion,
    "org.scalatest" %% "scalatest" % testVersion % "test"
  ) :+ scalaCheck
  lazy val gcp = Seq(
    "com.google.cloud" % "google-cloud-storage" % "2.3.0"
  )
  lazy val typeSafeConfig = "com.typesafe" % "config" % "1.4.1"

  lazy val catsVersion = "2.4.2"
  lazy val cats = Seq(
    "org.typelevel" %% "cats-core" % catsVersion,
    "org.typelevel" %% "cats-laws" % catsVersion,
    "org.typelevel" %% "cats-kernel" % catsVersion,
    "org.typelevel" %% "cats-kernel-laws" % catsVersion
  )

  lazy val monocleVersion = "2.1.0"
  lazy val monocle = Seq(
    "com.github.julien-truffaut" %% "monocle-core" % monocleVersion,
    "com.github.julien-truffaut" %% "monocle-macro" % monocleVersion
  )

  lazy val smileVersion = "2.6.0"
  lazy val smile = Seq(
    "com.github.haifengl" %% "smile-scala" % smileVersion
  )

  lazy val johnSVersion = "3.0.0"
  lazy val johnS = Seq(
    "com.johnsnowlabs.nlp" % "spark-nlp_2.12" % johnSVersion
  )
}
