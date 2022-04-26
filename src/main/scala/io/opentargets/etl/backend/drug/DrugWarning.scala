package io.opentargets.etl.backend.drug

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Drug warnings as produced by ChEMBL. Available since ChEMBL release 28.
  *
  * Drug warning are manually curated by ChEMBL according to the methodology outlined
  * [[https://pubs.acs.org/doi/pdf/10.1021/acs.chemrestox.0c00296 in this research paper]].
  *
  * The mappings to Meddra SOC codes are hard coded explicitly here, because they are bespoke categorisations
  * from the authors of the paper and '''do not''' map directly back to specific Meddra categories.
  */
object DrugWarning extends LazyLogging {
  def apply(df: DataFrame)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    logger.info("Preparing drug warnings")
    val publicationClassificationsDF = Seq(
      ("Carcinogenicity", 10029104),
      ("Cardiotoxicity", 10007541),
      ("Dermatological toxicity", 10040785),
      ("Gastrotoxicity", 10017947),
      ("Hematological toxicity", 10005329),
      ("Hepatotoxicity", 10019805),
      ("Immune system toxicity", 10021428),
      ("Infections", 10021881),
      ("Metabolism toxicity", 10027433),
      ("Misuse", 10022117),
      ("Musculoskeletal toxicity", 10028395),
      ("Nephrotoxicity", 10038359),
      ("Neurotoxicity", 10029205),
      ("Psychiatric toxicity", 10037175),
      ("Respiratory toxicity", 10038738),
      ("Teratogenicity", 10010331),
      ("Vascular toxicity", 10047065)
    ).toDF("toxicityClass", "meddraSocCode")

    val warningsDF = df.selectExpr(
      "_metadata.all_molecule_chembl_ids as chemblIds",
      "warning_class as toxicityClass",
      "warning_country as country",
      "warning_description as description",
      "warning_id as id",
      "warning_refs as references",
      "warning_type as warningType",
      "warning_year as year"
    )

    warningsDF.join(publicationClassificationsDF, Seq("toxicityClass"), "left_outer")
  }
}
