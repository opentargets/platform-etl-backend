import Dependencies._
import scala.sys.process.Process

val buildResolvers = Seq(
  "Typesafe Repo" at "https://repo.typesafe.com/typesafe/releases/",
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases",
  "Bintray Repo" at "https://dl.bintray.com/spark-packages/maven/"
)

ThisBuild / organization := "io.opentargets"
ThisBuild / version := sys.env.getOrElse("TAG", "0.0.0")
ThisBuild / scalaVersion := "2.12.20"

def jarName(name: String): String = s"$name-${sys.env.getOrElse("TAG", "0.0.0")}.jar"

scalacOptions ++= Seq("-Xlint:unused")

lazy val root = (project in file("."))
  .settings(
    name := "io-opentargets-etl-backend",
    resolvers ++= buildResolvers,
    libraryDependencies ++= etlDependencies,
    dependencyOverrides ++= Seq("org.scalanlp" %% "breeze" % "1.2"),
    testFrameworks += new TestFramework("minitest.runner.Framework"),
    Compile / run / mainClass := Some("io.opentargets.etl.Main"),
    Compile / packageBin / mainClass := Some("io.opentargets.etl.Main"),
    assembly / assemblyShadeRules := Seq(
      ShadeRule.rename("shapeless.**" -> "new_shapeless.@1").inAll
    ),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") =>
        MergeStrategy.filterDistinctLines
      case PathList("META-INF", "services", "org.apache.spark.sql.sources.DataSourceRegister") =>
        MergeStrategy.concat
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case _                             => MergeStrategy.first
    },
    assembly / assemblyJarName := jarName("etl")
  )
