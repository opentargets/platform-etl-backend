import Dependencies._

val buildResolvers = Seq(
  "Typesafe Repo" at "https://repo.typesafe.com/typesafe/releases/",
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases",
  "Bintray Repo" at "https://dl.bintray.com/spark-packages/maven/"
)

ThisBuild / organization := "io.opentargets"
ThisBuild / version := "1.0.0"
ThisBuild / scalaVersion := "2.12.12"

lazy val workflow = (project in file("workflow")).settings(
  name := "etl-workflow",
  libraryDependencies ++= workflowDependencies,
  scalacOptions ++= Seq("-feature",
                        "-deprecation",
                        "-unchecked",
                        "-language:postfixOps",
                        "-language:higherKinds",
                        "-Ypartial-unification"
  )
)

lazy val root = (project in file("."))
  .settings(
    name := "io-opentargets-etl-backend",
    resolvers ++= buildResolvers,
    libraryDependencies ++= etlDependencies,
    testFrameworks += new TestFramework("minitest.runner.Framework"),
    mainClass in (Compile, run) := Some("io.opentargets.etl.Main"),
    mainClass in (Compile, packageBin) := Some("io.opentargets.etl.Main"),
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") =>
        MergeStrategy.filterDistinctLines
      case PathList("META-INF", "services", "org.apache.spark.sql.sources.DataSourceRegister") =>
        MergeStrategy.concat
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case _                             => MergeStrategy.first
    }
  )
