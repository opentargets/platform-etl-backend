import $ivy.`com.typesafe:config:1.3.4`
import $ivy.`com.typesafe.scala-logging::scala-logging:3.9.0`
import $ivy.`ch.qos.logback:logback-classic:1.2.3`
import $ivy.`com.github.fommil.netlib:all:1.1.2`
import $ivy.`org.apache.spark::spark-core:2.4.3`
import $ivy.`org.apache.spark::spark-mllib:2.4.3`
import $ivy.`org.apache.spark::spark-sql:2.4.3`
import $ivy.`com.github.pathikrit::better-files:3.8.0`
import $ivy.`sh.almond::ammonite-spark:0.7.0`
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import com.typesafe.scalalogging.LazyLogging
import scala.collection.mutable.HashMap
import com.typesafe.config.{ConfigFactory, Config}
import scala.collection.JavaConversions._
import java.io._

/**
 Read the config file and provide the list of files to read.
*/
def getInputFiles(cfg: Config, step: String) : HashMap[String, String]  = { 
  val listFiles: HashMap[String, String] = HashMap()
  step match {
    case "disease" => cfg.getObject("input.disease.data-source-input").entrySet().map(entry => listFiles(entry.getKey)= entry.getValue.unwrapped().toString()) 
    case "target"  => cfg.getObject("input.target.data-source-input").entrySet().map(entry => listFiles(entry.getKey)= entry.getValue.unwrapped().toString()) 
	case "drug"    => cfg.getObject("input.drug.data-source-input").entrySet().map(entry => listFiles(entry.getKey)= entry.getValue.unwrapped().toString()) 
	case "all"     => cfg.getObject("input.all.data-source-input").entrySet().map(entry => listFiles(entry.getKey)= entry.getValue.unwrapped().toString()) 
    case _         => HashMap()
  }
  listFiles
}

/**
  Read the conf gile and return the Config cfg
*/
def getConfig(conf:String): Config = {
   val fileConfig = ConfigFactory.parseFile(new File(conf))
   val cfg = ConfigFactory.load(fileConfig)
   cfg
}

/**
  Spark common functions
*/
object SparkSessionWrapper extends LazyLogging {
  type WriterConfigurator = DataFrameWriter[Row] => DataFrameWriter[Row]

  // Return sensible defaults, possibly modified by configuration if necessary in the future. Eg. parquet
  private def defaultWriterConfigurator(): WriterConfigurator =
    (writer: DataFrameWriter[Row]) => writer.format("json").mode("overwrite")

  logger.info("Spark Session init")
  val sparkConf = new SparkConf()
    .set("spark.debug.maxToStringFields", "2000")
    .setAppName("etl-generation")
    .setMaster("local[*]")

  lazy val spark: SparkSession = {
    SparkSession.builder().config(sparkConf).getOrCreate
  }

  /** It creates an hashmap of dataframes.
   Es. inputsDataFrame {"disease", Dataframe} , {"target", Dataframe}
   Reading is the first step in the pipeline
  */
  def loader(
      inputFileConf: HashMap[String, String]
  ): HashMap[String, DataFrame] = {
	  
    logger.info("Load files into Hashmap Dataframe")
    val inputsDataFrame: HashMap[String, DataFrame] = HashMap()
    for ((step_key, step_filename) <- inputFileConf)
      inputsDataFrame(step_key) = loadFileToDF(step_filename)

    inputsDataFrame

  }

  def loadFileToDF(path: String): DataFrame = {
    val dataframe = spark.read.json(path)
    dataframe
  }

  def save(
      df: DataFrame,
      path: String,
      writerConfigurator: Option[WriterConfigurator] = None
  ): DataFrame = {
    val writer =
      writerConfigurator.getOrElse(defaultWriterConfigurator())(df.write)
    writer.save(path.toString)
    logger.info(s"Saved data to '$path'")
    df
  }

}
