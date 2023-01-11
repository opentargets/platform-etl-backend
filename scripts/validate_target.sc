import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode}

import scala.collection.mutable.ArrayBuffer

val ss: SparkSession = ???
import ss.implicits._

val path = "/home/jarrod/development/platform-etl-backend/data/output/target-safety-1745"

val df = ss.read.parquet(path)
// safety
val s = df.select('id, explode('safetyLiabilities) as "sl").select('id, $"sl.*")
val noHecatos = s.filter('datasource.contains("HeCaTos")).count()
val effects = s.select(explode('effects) as "e")
val refsWithURL = s.filter('url.isNotNull).select('datasource).distinct.count()
// homologues
val h = df.select('id, explode('homologues) as "h").select('id, $"h.*")
val noGeneSymbol = h.filter('targetGeneSymbol === "").count
val sameGeneAndSymbol = h.filter('id === "ENSG00000141510" && 'targetGeneId === "ENSOCUG00000001138")
  .filter('targetGeneId =!= 'targetGeneSymbol).count


// tep
val tep = df.filter('tep.isNotNull).count

// results

println(s"Total targets: ${df.count}  (should be ~61000)")
println(s"Total columns: ${df.columns.size} (should be 26)")
println(s"Safety - no hecatos: $noHecatos  (should be 0)")
println(s"Safety - 2 datasources have `url` field: $refsWithURL  (should be 2)")
println(s"Safety - effects contains general:")
effects.select($"e.dosing").distinct.show(false)

println(s"Homologue - entries with no gene symbl: $noGeneSymbol (should be 0)")
println(s"Homologue - ENSOCUG00000001138 on ENSG00000141510 has same targetGeneId and Symbol: $sameGeneAndSymbol (should be 0)")
println(s"Homologue - Same targetGeneId and targetGeneSymbol: ${h.filter('targetGeneSymbol === 'targetGeneId).count} (should be ~30k)")

println(s"Genes with TEP entries: ${tep} (should be ~40)")


