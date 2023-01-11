import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode}

val ss: SparkSession = ???
import ss.implicits._

val path = "/home/jarrod/development/platform-etl-backend/data/output/"
val succeeded = path + "targetValidated"
val failed = path + "targetValidatedFailed"
val mpSuccessPath = succeeded + "/mousePhenotypes"
val mpFailedPath = failed + "/mousePhenotypes"
val target_df: DataFrame = ???

val dfs = ss.read.parquet(mpSuccessPath)
val dff = ss.read.parquet(mpFailedPath)

dfs.join(target_df, col("id") === col("targetFromSourceId"), "left_anti").count