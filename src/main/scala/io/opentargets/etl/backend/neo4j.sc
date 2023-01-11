import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.{col, explode, lit, struct}

val ss: SparkSession = ???
import ss.implicits._

// facet reactome
val t: DataFrame = ???
val pw = t.select(col("id"),
                  functions.transform(col("pathways"),
                                      r =>
                                        struct(r.getField("topLevelTerm") as "topLevelTerm",
                                               r.getField("pathway") as "pathway")) as "pathways")

val targetPathways = pw
  .select(col("id"), explode(col("pathways")) as "pw")
  .select(col("id"), col("pw.*"))
  .filter(col("pathways").isNotNull && col("topLevelTerm").isNotNull)

targetPathways
  .coalesce(1)
  .write
  .option("header", "true")
  .csv("targetPathwayN4J")

import ss.implicits._

val ontology = ss.read.json("ontology-efo*")

val df = ontology.select(
  col("id"),
  explode(col("parents")) as "parent")
  .orderBy("parent")
val parents = df.select(col("parent") as "p")
val d = parents.join(df.select('id, 'name), 'id === 'p, "left_outer")
val p = df.select(col("parent") as "p'")
  .join(df, df("id") === col("p'"), "left_outer")
  .select(
    col("parent"),
    col("name") as "pname"
  )
  .distinct
  .orderBy("parent")

val rel = df.join(p, Seq("parent"))

val df = Seq(
  (0, "A", Array("_age=10", "_city=A")),
  (1, "B", Array("_age=20", "_city=B")),
  (2, "C", Array("tag=XYZ", "_city=BC")),
).toDF("id", "name", "properties")





