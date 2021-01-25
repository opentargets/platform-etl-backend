import coursierapi._

interp.repositories() ++= Seq(
  MavenRepository
    .of("http://dl.bintray.com/spark-packages/maven")
)
