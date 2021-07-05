package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.MousePhenotypes.compute
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import org.apache.spark.sql.functions.{
  array,
  array_distinct,
  broadcast,
  col,
  collect_list,
  collect_set,
  explode,
  explode_outer,
  flatten,
  regexp_extract,
  split,
  struct,
  transform,
  typedLit
}
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}

object AdverseOutcomePathway extends LazyLogging {
  def computeAop(context: ETLSessionContext): DataFrame = {
    implicit val ss: SparkSession = context.sparkSession

    /**
      * The input xml file has several sections which Spark needs to know about to read in the
      * whole file. Each of the dataSources below is a `rowTag` and represents it's own Dataframe.
      */
    val path = context.configuration.aop.input.path

    def aopDFReader: String => DataFrame =
      (dataSource: String) =>
        ss.read
          .format("com.databricks.spark.xml")
          .option("rootTag", "data")
          .option("rowTag", dataSource)
          .load(path)

    val dataSources = Seq(
      "chemical",
      "biological-object",
      "biological-process",
      "biological-action",
      "stressor",
      "taxonomy",
      "key-event",
      "key-event-relationship",
      "aop",
      "vendor-specific"
    ).map(ds => ds -> aopDFReader(ds)).toMap

    lazy val vendorSpecRawDF = dataSources("vendor-specific")
    lazy val aopRawDF = dataSources("aop")
    lazy val chemicalRawDF = dataSources("chemical")
    lazy val stressorRawDF = dataSources("stressor")
    lazy val keyEventRawDF = dataSources("key-event")

    val xrefId = "xrefId"
    val ke = "key-event"
    val mie = "molecular-initiating-event"
    val ao = "adverse-outcome"

    /*
    The raw data uses random strings for identifiers, as the actual identifiers are simple numbers. Presumably the AOP
    project stored their data with these custom identifiers to prevent clashes (eg chemical and key-event can both have
    ID 103). We need these to convert back into the 'native' IDs to link back to the website.

    See `require` line in method for list of valid fields.
     */
    def getIdLookup: String => DataFrame =
      (field: String) => {
        require(
          Seq(
            "aop",
            "biological-action",
            "biological-object",
            "biological-process",
            "chemical",
            "key-event",
            "key-event-relationship",
            "stressor",
            "taxonomy"
          ).contains(field),
          s"Field $field is not valid."
        )
        vendorSpecRawDF
          .select(explode(col(s"$field-reference")) as "r")
          .select(col("r._id") as xrefId, col("r._aop-wiki-id") as "aopId")
      }

    val aopIdLookup = getIdLookup("aop")
    val keyEventIdLookup = getIdLookup("key-event")
    val chemicalIdLookupDF = getIdLookup("chemical")

    val aopIdRawDF = aopRawDF
      .select(
        col("_id"),
        col("title"),
        col("status.oecd-status") as "status",
        col("aop-stressors.aop-stressor") as "aopStressors",
        explode_outer(col("molecular-initiating-event._key-event-id")) as mie,
        col("adverse-outcome._key-event-id") as ao,
        transform(split(col("key-events"), "/key-event"), (c: Column) => {
          regexp_extract(c, "\\w{8}-\\w{4}-\\w{4}-\\w{4}-\\w{12}", 0)
        })
          as ke,
      )
      .withColumn(ao, explode_outer(col(ao)))
      .withColumn(ke, explode_outer(col(ke)))

    // map raw id strings into numerical ids.
    val aopDF = Seq(ke, mie, ao)
      .foldLeft(aopIdRawDF)((df, c) => {
        df.join(broadcast(keyEventIdLookup), keyEventIdLookup(xrefId) === col(c), "left_outer")
          .drop(xrefId, c)
          .withColumnRenamed("aopId", c)
          .withColumn(c,
                      struct(
                        col(c) as "id",
                        typedLit(c) as "type"
                      ))
      })
      .select(
        col("_id") as xrefId,
        col("title"),
        col("status"),
        functions.filter(array(ke, mie, ao), x => x("id").isNotNull) as "keyEvents"
      )
      .groupBy(xrefId, "title", "status")
      .agg(flatten(collect_set("keyEvents")) as "keyEvents")
      .withColumn("keyEvents", array_distinct(col("keyEvents")))
      // convert id hash to meaningful id
      .join(broadcast(aopIdLookup), Seq(xrefId))
      .withColumnRenamed("aopId", "id")

    // CHEMICAL
    val chemDF = chemicalRawDF
      .select(
        col("_id") as "_chemical-id",
        col("jchem-inchi-key") as "inchiKey",
        col("preferred-name") as "preferredName",
        col("synonyms.synonym") as "synonyms"
      )
      .join(broadcast(chemicalIdLookupDF), col("_chemical-id") === col(xrefId))
      .drop(xrefId)
      .withColumnRenamed("aopId", "chemicalId")

    // STRESSOR
    // extract quality assurance stressor field from aopDF
    val stressFromAop = aopIdRawDF
      .select(col("_id"), explode(col("aopStressors")) as "x")
      .select(col("_id") as "_id_aop",
              col("x._stressor-id") as "_stressor-id",
              col("x.evidence") as "qualityAssurance")
    // select needed fields and turn raw stressor id into legible id
    val stressDF = stressorRawDF
      .select(
        col("_id"),
        col("name"),
        explode(col("chemicals.chemical-initiator._chemical-id")) as "_chemical-id"
      )
      .join(broadcast(stressFromAop), col("_id") === col("_stressor-id"))
      .drop("_id", "_stressor-id", "description")

    // join chemical and stressor data frames
    val stressorsDF = stressDF
      .join(broadcast(chemDF), Seq("_chemical-id"), "left_outer")
      .drop("_chemical-id")
      .distinct

    // group by aop id so we can merge with aop dataframe
    val stressorsAggregatedByAopId = stressorsDF
      .groupBy(col("_id_aop"))
      .agg(
        collect_list(
          struct(
            col("name"),
            col("qualityAssurance"),
            col("chemicalId"),
            col("inchiKey"),
            col("preferredName"),
            col("synonyms")
          )) as "stressors")

    // COMBINE
    aopDF
      .join(stressorsAggregatedByAopId, col("_id_aop") === col(xrefId), "left_outer")
      .drop(xrefId, "_id_aop")
  }

  def apply()(implicit context: ETLSessionContext): IOResources = {

    implicit val ss: SparkSession = context.sparkSession

    val aopDF = computeAop(context)

    val dataframesToSave: IOResources = Map(
      "aopDF" -> IOResource(aopDF, context.configuration.aop.output)
    )

    IoHelpers.writeTo(dataframesToSave)

  }
}
