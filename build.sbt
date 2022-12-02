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
  ),
  coverageEnabled := true,
  logLevel in assembly := Level.Info,
  mainClass in assembly := Some("io.opentargets.workflow.cli.OpenTargetsCliApp"),
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", "io.netty.versions.properties") =>
      MergeStrategy.discard
    case PathList(ps @ _*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
      MergeStrategy.rename
    case PathList("META-INF", xs @ _*) =>
      (xs map {
        _.toLowerCase
      }) match {
        case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
          MergeStrategy.discard
        case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
          MergeStrategy.discard
        case "plexus" :: xs =>
          MergeStrategy.discard
        case "versions" :: xs => MergeStrategy.discard
        case "services" :: xs =>
          MergeStrategy.filterDistinctLines
        case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
          MergeStrategy.filterDistinctLines
        case _ => MergeStrategy.deduplicate
      }
    case "module-info.class" => MergeStrategy.filterDistinctLines
    case _                   => MergeStrategy.deduplicate
  }
)
lazy val root = (project in file("."))
  .settings(
    name := "io-opentargets-etl-backend",
    resolvers ++= buildResolvers,
    libraryDependencies ++= etlDependencies,
    testFrameworks += new TestFramework("minitest.runner.Framework"),
    mainClass in (Compile, run) := Some("io.opentargets.etl.Main"),
    mainClass in (Compile, packageBin) := Some("io.opentargets.etl.Main"),
    assemblyShadeRules in assembly := Seq(
      ShadeRule.rename("shapeless.**" -> "new_shapeless.@1").inAll
    ),
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") =>
        MergeStrategy.filterDistinctLines
      case PathList("META-INF", "services", "org.apache.spark.sql.sources.DataSourceRegister") =>
        MergeStrategy.concat
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case _                             => MergeStrategy.first
    }
  )
