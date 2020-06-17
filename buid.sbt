import Dependencies._

val buildResolvers = Seq(
  //    "Local Maven Repository"    at "file://"+Path.userHome.absolutePath+"/.m2/repository",
  //    "Maven repository"          at "http://download.java.net/maven/2/",
  "Typesafe Repo" at "https://repo.typesafe.com/typesafe/releases/",
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases"
)

lazy val root = (project in file("."))
  .settings(
    inThisBuild(
      List(
        organization := "io.opentargets",
        scalaVersion := "2.12.10"
      )
    ),
    name := "io-opentargets-etl-backend",
    version := "0.3.0",
    resolvers ++= buildResolvers,
    libraryDependencies += scalaCheck,
    libraryDependencies ++= sparkDeps,
    libraryDependencies += scalaLoggingDep,
    libraryDependencies += scalaLogging,
    libraryDependencies ++= aoyi,
    libraryDependencies += betterFiles,
    libraryDependencies += typeSafeConfig,
    libraryDependencies ++= configDeps,
    testFrameworks += new TestFramework("minitest.runner.Framework"),
//    assemblyShadeRules in assembly := {
//      case ShadeRule.rename("com.google.**" -> "org.apache.gearpump.google.@1").inAll
//    },
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") =>
        MergeStrategy.filterDistinctLines
      case PathList("META-INF", "services", "org.apache.spark.sql.sources.DataSourceRegister") =>
        MergeStrategy.concat
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case _                             => MergeStrategy.first
    }
  )
