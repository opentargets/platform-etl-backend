import sbt._

object Dependencies {

  lazy val aoyi = Seq(
    "com.lihaoyi" %% "pprint" % "0.6.0"
  )

  lazy val betterFiles = "com.github.pathikrit" %% "better-files-akka" % "3.9.1"

  lazy val codeDeps = Seq(
    "com.beachape" %% "enumeratum" % "1.6.1",
    "com.github.scopt" %% "scopt" % "3.7.1"
  )

  lazy val configDeps = Seq(
    "org.yaml" % "snakeyaml" % "1.21",
    "com.github.pureconfig" %% "pureconfig" % "0.12.3"
  )

  lazy val loggingDeps = Seq(
    "ch.qos.logback" % "logback-classic" % "1.2.3",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
  )

  lazy val scalaCheck = "org.scalacheck" %% "scalacheck" % "1.14.3"

  val sparkVersion = "3.0.1"
  lazy val sparkDeps = Seq(
    "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly (),
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-graphx" % sparkVersion,
    "org.apache.spark" %% "spark-mllib" % sparkVersion
  )

  lazy val testingDeps = Seq(
    "org.scalactic" %% "scalactic" % "3.0.8",
    "org.scalatest" %% "scalatest" % "3.0.8" % "test"
  )

  lazy val typeSafeConfig = "com.typesafe" % "config" % "1.4.0"
}
