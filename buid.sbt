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
        scalaVersion := "2.12.10",
        version := "0.2.1"
      )
    ),
    name := "io-opentargets-etl-backend",
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
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs @ _*)                => MergeStrategy.discard
      case PathList("org", "aopalliance", xs @ _*)      => MergeStrategy.last
      case PathList("javax", "inject", xs @ _*)         => MergeStrategy.last
      case PathList("javax", "servlet", xs @ _*)        => MergeStrategy.last
      case PathList("javax", "activation", xs @ _*)     => MergeStrategy.last
      case PathList("org", "apache", xs @ _*)           => MergeStrategy.last
      case PathList("com", "google", xs @ _*)           => MergeStrategy.last
      case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
      case PathList("com", "codahale", xs @ _*)         => MergeStrategy.last
      case PathList("com", "yammer", xs @ _*)           => MergeStrategy.last
      case PathList("org", "slf4j", "impl", xs @ _*)    => MergeStrategy.last
      case "module-info.class"                          => MergeStrategy.last
      case "reference-overrides.conf"                   => MergeStrategy.concat
      case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" =>
        MergeStrategy.concat
      case "about.html"        => MergeStrategy.rename
      case "overview.html"     => MergeStrategy.rename
      case "plugin.properties" => MergeStrategy.last
      case "log4j.properties"  => MergeStrategy.last
      case "git.properties"    => MergeStrategy.last
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )
