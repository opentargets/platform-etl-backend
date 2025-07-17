package io.opentargets.etl.backend.evidence

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.spark.Helpers.{mkFlattenArray, mkRandomPrefix}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

object Evidence extends LazyLogging {
  val rawScoreColumnName: String = "resourceScore"

  object UDFs {

    /** apply the function f(x) to n using and old (start_range) and a new range. pValue inRangeMin
      * and inRangeMax have log10 applied before f(x) is calculated where f(x) = (dNewRange /
      * dOldRange * (n - old_range_lower_bound)) + new_lower if cap is True then f(n) will be capped
      * to new range boundaries
      */
    def pValueLinearRescaling(
        pValue: Double,
        inRangeMin: Double,
        inRangeMax: Double,
        outRangeMin: Double,
        outRangeMax: Double
    ): Double = {
      val pValueLog = Math.log10(pValue)
      val inRangeMinLog = Math.log10(inRangeMin)
      val inRangeMaxLog = Math.log10(inRangeMax)

      linearRescaling(pValueLog, inRangeMinLog, inRangeMaxLog, outRangeMin, outRangeMax)
    }

    def linearRescaling(
        value: Double,
        inRangeMin: Double,
        inRangeMax: Double,
        outRangeMin: Double,
        outRangeMax: Double
    ): Double = {
      val delta1 = inRangeMax - inRangeMin
      val delta2 = outRangeMax - outRangeMin

      val score: Double = (delta1, delta2) match {
        case (d1, d2) if d1 != 0d => (d2 * (value - inRangeMin) / d1) + outRangeMin
        case (0d, 0d)             => value
        case (0d, _)              => outRangeMin
      }

      Math.max(Math.min(score, outRangeMax), outRangeMin)
    }
  }

  def prepare(df: DataFrame)(implicit ss: SparkSession): DataFrame = {
    import ss.implicits._

    ss.sqlContext.udf.register("linear_rescale", UDFs.linearRescaling _)
    ss.sqlContext.udf.register("pvalue_linear_score", UDFs.pValueLinearRescaling _)
    ss.sqlContext.udf
      .register("pvalue_linear_score_default", UDFs.pValueLinearRescaling(_, 1, 1e-10, 0, 1))

    val prepared = df.withColumn("sourceId", $"datasourceId")

    // you cannot operate with a column name that is actually not present in the columns schema
    if (prepared.columns.contains(rawScoreColumnName))
      prepared.withColumn(rawScoreColumnName, col(rawScoreColumnName).cast(DoubleType))
    else
      prepared.withColumn(rawScoreColumnName, lit(null).cast(DoubleType))
  }

  def excludeByBiotype(
      df: DataFrame,
      targets: DataFrame,
      columnName: String,
      targetIdCol: String,
      datasourceIdCol: String
  )(implicit context: ETLSessionContext): DataFrame = {
    def mkLUT(df: DataFrame): DataFrame =
      df.select(
        col("id").as(targetIdCol),
        col("biotype")
      )

    logger.info("filter evidences by target biotype exclusion list - default is nothing to exclude")

    val tName = mkRandomPrefix()
    val btsCol = "biotypes"
    implicit val session: SparkSession = context.sparkSession
    import session.implicits._
    val evsConf = broadcast(
      context.configuration.steps.evidence.dataSources
        .filter(_.excludedBiotypes.isDefined)
        .map(ds => (ds.id, ds.excludedBiotypes.get))
        .toDF(datasourceIdCol, btsCol)
        .withColumn("biotype", explode(col(btsCol)))
        .withColumn(tName, lit(true))
        .drop(btsCol)
    )

    val lut = broadcast(
      targets
        .transform(mkLUT)
        .orderBy(col(targetIdCol).asc)
    )

    val filtered = df
      .join(lut, Seq(targetIdCol), "left")
      .join(evsConf, Seq(datasourceIdCol, "biotype"), "left")
      .withColumn(columnName, col(tName).isNotNull)
      .drop("biotype", tName)

    filtered
  }

  def resolveTargets(
      df: DataFrame,
      targets: DataFrame,
      columnName: String,
      fromId: String,
      toId: String
  )(implicit context: ETLSessionContext): DataFrame = {
    def generateTargetsLUT(df: DataFrame): DataFrame =
      df.select(
        col("id").as("tId"),
        array_distinct(
          mkFlattenArray(
            array(col("id")),
            col("proteinIds.id"),
            array(col("approvedSymbol"))
          )
        ).as("rIds")
      ).withColumn("rId", explode(col("rIds")))
        .select("tId", "rId")

    logger.info("target resolution evidences and write to out the ones didn't resolve")

    implicit val session: SparkSession = context.sparkSession
    import session.implicits._

    val lut = broadcast(
      targets
        .transform(generateTargetsLUT)
        .orderBy($"rId".asc)
        .repartition($"rId")
    )

    val fromIdC = col(fromId)

    val resolved = df
      .join(lut, fromIdC === col("rId"), "left_outer")
      .withColumn(columnName, col("tId").isNotNull)
      .withColumn(toId, coalesce(col("tId"), fromIdC))
      .drop("tId", "rId")

    resolved
  }

  def resolveDiseases(
      df: DataFrame,
      diseases: DataFrame,
      columnName: String,
      fromId: String,
      toId: String
  )(implicit context: ETLSessionContext): DataFrame = {
    logger.info("disease resolution evidences and write to out the ones didn't resolve")

    implicit val session: SparkSession = context.sparkSession
    import session.implicits._

    val lut = broadcast(
      diseases
        .select(
          col("id").as("efoId"),
          explode(
            concat(
              array(col("id")),
              coalesce(col("obsoleteTerms"), typedLit(Array.empty[String]))
            )
          ).as("did")
        )
        .orderBy($"dId".asc)
        .repartition($"dId")
    )

    val fromIdC = col(fromId)

    val resolved = df
      .join(lut, fromIdC === col("dId"), "left_outer")
      .withColumn(columnName, col("dId").isNotNull)
      .withColumn(toId, coalesce(col("efoId"), fromIdC))
      .drop("dId", "efoId")

    resolved
  }

  /**
   * Resolves publication dates for evidence based on literature identifiers.
   * 
   * This function takes evidence records with literature arrays and matches them
   * against a publication date mapping to add publicationDate and evidenceDate columns.
   * The evidenceDate uses publicationDate when available, falling back to releaseDate.
   *
   * @param df the evidence DataFrame containing literature arrays
   * @param publication_date_mapping DataFrame with publication dates mapped to identifiers
   * @param context the ETL session context containing Spark session
   * @return DataFrame with added publicationDate and evidenceDate columns
   */
  def resolvePublicationDates(
      df: DataFrame,
      publication_date_mapping: DataFrame
  )(implicit context: ETLSessionContext): DataFrame = {
    logger.info("resolve publication dates for evidences based on literature identifiers")

    implicit val session: SparkSession = context.sparkSession

    // Filter for MED, AGR and pre-prints (PPR) and create temp view called pub_data:
     val processedPublicationData = publication_date_mapping
      .filter(col("source").isin("MED", "PPR", "AGR"))
      .select(
        col("firstPublicationDate").alias("publicationDate"),
        explode(
          expr("filter(array(pmid, id, pmcid), x -> x is not null)")
        ).alias("publicationId")
      )

    val processedDatesLut = broadcast(processedPublicationData.orderBy(col("publicationId").asc))

    // Process evidence with literature field
    val evidenceWithPubIds = df
      .select(col("id"), explode(col("literature")).as("publicationId"))
      // Clean publication IDs - trim and uppercase
      .withColumn(
        "publicationId",
        upper(trim(col("publicationId")))
      )

    // Join with publication dates
    val datedEvidence = evidenceWithPubIds
      .join(processedDatesLut, Seq("publicationId"), "inner")
      // Rank by publication date to find earliest publication for each evidence
      .withColumn(
        "rank",
        row_number().over(
          Window.partitionBy("id").orderBy(col("publicationDate").asc)
        )
      )
      // Filter for the first (earliest) publication for each evidence
      .filter(col("rank") === 1)
      .select("id", "publicationDate")

    val datedEvidenceLut = broadcast(datedEvidence.orderBy(col("id").asc))

    // Join original evidence with dated evidence
    val resolved = df
      .join(datedEvidenceLut, Seq("id"), "left_outer")
      .withColumn(
        "evidenceDate",
        coalesce(
          col("publicationDate"), 
          col("releaseDate")
        )
      )

    resolved
  }

  def normaliseDatatypes(df: DataFrame)(implicit context: ETLSessionContext): DataFrame = {
    implicit val ss: SparkSession = context.sparkSession
    import ss.implicits._

    logger.info(
      "build a LUT table for the datatypes to make sure every datasource has one datatype id"
    )
    val config = context.configuration.steps.evidence

    val dsId = "datasourceId"
    val colName = "datatypeId"
    val customColName = "customDatatypeId"
    val defaultDTId = config.datatypeId

    val dfWithDT = if (df.columns.contains(colName)) {
      df.withColumn(
        colName,
        when(col(colName).isNull, lit(defaultDTId))
          .otherwise(col(colName))
      )
    } else {
      df.withColumn(colName, lit(defaultDTId))
    }

    val customDTs =
      broadcast(
        config.dataSources
          .filter(_.datatypeId.isDefined)
          .map(x => x.id -> x.datatypeId.get)
          .toDF(dsId, customColName)
          .orderBy(col(dsId).asc)
      )

    dfWithDT
      .join(customDTs, Seq(dsId), "left_outer")
      .withColumn(colName, coalesce(col(customColName), col(colName)))
      .drop(customColName)
  }

  def generateHashes(df: DataFrame, columnName: String)(implicit
      context: ETLSessionContext
  ): DataFrame = {
    val config = context.configuration.steps.evidence

    logger.info("Validate each evidence: generating a hash to check for duplicates")

    val commonReqFields = config.uniqueFields.toSet

    val dataTypes: List[(Column, List[Column])] = config.dataSources
      .map(dataType =>
        (col("sourceId") === dataType.id) ->
          (commonReqFields ++ dataType.uniqueFields.toSet).toList.sorted
            .map(x => when(expr(x).isNotNull, expr(x).cast(StringType)).otherwise(""))
      )

    val defaultDts = commonReqFields.toList.sorted.map { x =>
      when(col(x).isNotNull, col(x).cast(StringType)).otherwise("")
    }

    val hashes = dataTypes.tail
      .foldLeft(when(dataTypes.head._1, sha1(concat(dataTypes.head._2: _*)))) { case op =>
        op._1.when(op._2._1, sha1(concat(op._2._2: _*)))
      }
      .otherwise(sha1(concat(defaultDts: _*)))

    df.withColumn(columnName, hashes)
  }

  def hashLongVariantIds(df: DataFrame, columnName: String, threshold: Int = 300): DataFrame = {
    logger.info("Hash long variantIds")

    val variantId = col(columnName)

    df
      .withColumn("chr",
                  regexp_extract(variantId, "^([0-9XYMT]{1,2})_([0-9]+)_([ACGTN]+)_([ACGTN]+)$", 1)
      )
      .withColumn("pos",
                  regexp_extract(variantId, "^([0-9XYMT]{1,2})_([0-9]+)_([ACGTN]+)_([ACGTN]+)$", 2)
      )
      .withColumn(
        columnName,
        when(col("chr").isNull || col("pos").isNull,
             concat(lit("OTVAR_"), md5(variantId).cast("string"))
        )
          .when(length(variantId) > threshold,
                concat_ws("_", lit("OTVAR"), col("chr"), col("pos"), md5(variantId).cast("string"))
          )
          .otherwise(variantId)
      )
      .drop("chr", "pos")
  }

  def score(df: DataFrame, columnName: String)(implicit context: ETLSessionContext): DataFrame = {
    logger.info("score each evidence and mark unscored ones")
    val config = context.configuration.steps.evidence

    val dts = config.dataSources.map { dt =>
      (col("sourceId") === dt.id) -> expr(dt.scoreExpr)
    }

    val scores = dts.tail
      .foldLeft(when(dts.head._1, dts.head._2)) { case op =>
        op._1.when(op._2._1, op._2._2)
      }
      .otherwise(expr(config.scoreExpr))

    df.withColumn(columnName, scores)
  }

  def checkNullifiedScores(df: DataFrame,
                           scoreColumnName: String,
                           columnName: String
  ): DataFrame = {
    val idC = col(scoreColumnName)

    df.withColumn(columnName, idC.isNull)
  }

  def markDuplicates(df: DataFrame, hashColumnName: String, columnName: String): DataFrame = {
    val idC = col(hashColumnName)
    val w = Window.partitionBy(col("sourceId"), idC).orderBy(idC.asc)

    df.withColumn("_idRank", row_number().over(w))
      .withColumn(columnName, when(col("_idRank") > 1, typedLit(true)).otherwise(false))
      .drop("_idRank")
  }

  def stats(df: DataFrame, aggs: Seq[Column])(implicit context: ETLSessionContext): DataFrame = {
    import context.sparkSession.implicits._

    df.groupBy($"sourceId")
      .agg(aggs.head, aggs.tail: _*)
  }

  def compute()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession

    val evidencesSec = context.configuration.steps.evidence

    logger.info(s"Executing evidence step with data-types: ${evidencesSec.dataSources.map(_.id)}")

    val dfs = IoHelpers.readFrom(evidencesSec.input)

    val rt = "resolvedTarget"
    val rd = "resolvedDisease"
    val md = "markedDuplicate"
    val id = "id"
    val sc = "score"
    val ns = "nullifiedScore"
    val xb = "excludedBiotype"
    val targetId = "targetId"
    val diseaseId = "diseaseId"
    val fromTargetId = "targetFromSourceId"
    val fromDiseaseId = "diseaseFromSourceMappedId"
    val datasourceId = "datasourceId"
    val variantId = "variantId"
    val varIdLenThreshold = 300

    val combinedEvidencesDF =
      dfs("raw_input_evidences").data
        .unionByName(dfs("raw_intermediate_evidences").data, allowMissingColumns = true)

    val transformedDF = combinedEvidencesDF
      .transform(prepare)
      .transform(resolveTargets(_, dfs("targets").data, rt, fromTargetId, targetId))
      .transform(resolveDiseases(_, dfs("diseases").data, rd, fromDiseaseId, diseaseId))
      .transform(resolvePublicationDates(_, dfs("literature_dating_input").data))
      .transform(excludeByBiotype(_, dfs("targets").data, xb, targetId, datasourceId))
      .transform(normaliseDatatypes _)
      .transform(generateHashes(_, id))
      .transform(hashLongVariantIds(_, variantId, varIdLenThreshold))
      .transform(score(_, sc))
      .transform(checkNullifiedScores(_, sc, ns))
      .transform(markDuplicates(_, id, md))
      .transform(
        DirectionOfEffect(_, dfs("targets").data, dfs("mechanism_of_action").data)
      )
      .persist(StorageLevel.DISK_ONLY)

    val okFitler = col(rt) and col(rd) and !col(md) and !col(ns) and !col(xb)

    val outputPathConf = context.configuration.steps.evidence.output
    Map(
      "ok" -> IOResource(
        transformedDF.filter(okFitler).drop(rt, rd, md, ns, xb),
        outputPathConf("succeeded")
      ),
      "failed" -> IOResource(transformedDF.filter(not(okFitler)), outputPathConf("failed"))
    )
  }

  def apply()(implicit context: ETLSessionContext): IOResources = {
    val processedEvidences = compute()
    IoHelpers.writeTo(processedEvidences)

  }
}
