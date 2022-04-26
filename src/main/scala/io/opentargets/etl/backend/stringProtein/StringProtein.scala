package io.opentargets.etl.backend.stringProtein

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

/*
This object returns a specific schema for interaction use.
The test unit helps to check the output schema
root
 |-- interaction: struct (nullable = true)
 |    |-- causal_interaction: boolean (nullable = true)
 |    |-- evidence: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- evidence_score: long (nullable = true)
 |    |    |    |-- interaction_detection_method_mi_identifier: string (nullable = true)
 |    |    |    |-- interaction_detection_method_short_name: string (nullable = true)
 |    |    |    |-- interaction_identifier: string (nullable = true)
 |    |    |    |-- pubmed_id: string (nullable = true)
 |    |-- interaction_score: long (nullable = true)
 |-- interactorA: struct (nullable = true)
 |    |-- biological_role: string (nullable = true)
 |    |-- id: string (nullable = true)
 |    |-- id_source: string (nullable = true)
 |    |-- organism: struct (nullable = true)
 |    |    |-- mnemonic: string (nullable = true)
 |    |    |-- scientific_name: string (nullable = true)
 |    |    |-- taxon_id: long (nullable = true)
 |-- interactorB: struct (nullable = true)
 |    |-- biological_role: string (nullable = true)
 |    |-- id: string (nullable = true)
 |    |-- id_source: string (nullable = true)
 |    |-- organism: struct (nullable = true)
 |    |    |-- mnemonic: string (nullable = true)
 |    |    |-- scientific_name: string (nullable = true)
 |    |    |-- taxon_id: long (nullable = true)
 |-- source_info: struct (nullable = true)
 |    |-- database_version: string (nullable = true)
 |    |-- source_database: string (nullable = true)


 */
object StringProtein extends Serializable with LazyLogging {

  def apply(stringDataset: DataFrame, scorethreshold: Int)(implicit ss: SparkSession): DataFrame = {
    import ss.implicits._

    logger.info("Compute string protein dataset threshold: " + scorethreshold.toString)

    val stringDatasetFiltered = stringDataset
      .withColumn("interaction_score", ltrim(col("combined_score")).cast(IntegerType))
      .filter(col("interaction_score") >= scorethreshold)

    val colsEvidence = Seq(
      ("coexpression", "MI:2231"),
      ("cooccurence", "MI:2231"),
      ("neighborhood", "MI:0057"),
      ("fusion", "MI:0036"),
      ("homology", "MI:2163"),
      ("experimental", "MI:0591"),
      ("database", ""),
      ("textmining", "MI:0110")
    )

    val stringDatasetEvidence = colsEvidence.foldLeft(stringDatasetFiltered)((acc, nxt) => {
      acc.withColumn(
        "e_" + nxt._1,
        struct(
          lit(nxt._1) as "interaction_detection_method_short_name",
          lit(nxt._2) as "interaction_detection_method_mi_identifier",
          col(nxt._1).cast(LongType) as "evidence_score",
          lit(null) as "interaction_identifier",
          lit(null) as "pubmed_id"
        )
      )
    })

    stringDatasetEvidence
      .filter(col("protein1").contains("9606."))
      .filter(col("protein2").contains("9606."))
      .withColumn("id_source_p1", regexp_replace(col("protein1"), "9606.", ""))
      .withColumn("id_source_p2", regexp_replace(col("protein2"), "9606.", ""))
      .withColumn("biological_role", lit("unspecified role"))
      .withColumn("id_source", lit("ensembl_protein"))
      .withColumn(
        "organism",
        struct(
          lit("human") as "mnemonic",
          lit("Homo sapiens") as "scientific_name",
          lit("9606").cast("bigint") as "taxon_id"
        )
      )
      .withColumn(
        "interactorA",
        struct(
          col("id_source"),
          col("biological_role"),
          col("id_source_p1") as "id",
          col("organism")
        )
      )
      .withColumn(
        "interactorB",
        struct(
          col("id_source"),
          col("biological_role"),
          col("id_source_p2") as "id",
          col("organism")
        )
      )
      .withColumn(
        "source_info",
        struct(lit("11") as "database_version", lit("string") as "source_database")
      )
      .withColumn("causal_interaction", lit("False").cast(BooleanType))
      .drop("protein1", "protein2", "id_source_p1", "id_source_p2", "biological_role", "id_source")
      .withColumn(
        "all_evidence",
        array(
          col("e_textmining"),
          col("e_database"),
          col("e_experimental"),
          col("e_fusion"),
          col("e_neighborhood"),
          col("e_cooccurence"),
          col("e_coexpression"),
          col("e_homology")
        )
      )
      .withColumn(
        "interaction",
        struct(
          col("interaction_score"),
          col("causal_interaction"),
          col("all_evidence") as "evidence"
        )
      )
      .drop(
        "combined_score",
        "textmining",
        "database",
        "experimental",
        "fusion",
        "neighborhood",
        "cooccurence",
        "coexpression",
        "homology",
        "e_textmining",
        "e_database",
        "e_experimental",
        "e_fusion",
        "e_neighborhood",
        "e_cooccurence",
        "e_coexpression",
        "e_homology",
        "all_evidence",
        "interaction_score",
        "causal_interaction",
        "organism"
      )

  }

}
