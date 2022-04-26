package io.opentargets.etl.backend.Drug

import io.opentargets.etl.backend.EtlSparkUnitTest
import io.opentargets.etl.backend.drug.MechanismOfAction
import org.apache.spark.sql.functions.{col, size => sparkSize}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class MechanismOfActionTest extends EtlSparkUnitTest {

  val testMoAInput =
    raw"""[{"actionType":"ANTAGONIST","chemblIds":["CHEMBL13","CHEMBL1200337","CHEMBL1200840","CHEMBL2062335","CHEMBL545787","CHEMBL3186768","CHEMBL2356097","CHEMBL3989566","CHEMBL1868892"],"mechanismOfAction":"Beta-1 adrenergic receptor antagonist","references":[{"ids":["setid=31d8610a-ab22-49e5-a762-6b08244ee93a"],"source":"DailyMed","urls":["http://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=31d8610a-ab22-49e5-a762-6b08244ee93a"]}],"targetName":"Beta-1 adrenergic receptor","targetType":"single protein","targets":["ENSG00000043591"]},
  {"actionType":"ANTAGONIST","chemblIds":["CHEMBL13","CHEMBL1200337","CHEMBL1200840","CHEMBL2062335","CHEMBL545787","CHEMBL3186768","CHEMBL2356097","CHEMBL3989566","CHEMBL1868892"],"mechanismOfAction":"Beta-1 adrenergic receptor antagonist","references":[{"ids":["setid=31d8610a-ab22-49e5-a762-6b08244ee93a"],"source":"DailyMed","urls":["http://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=31d8610a-ab22-49e5-a762-6b08244ee93a"]}],"targetName":"Beta-1 adrenergic receptor","targetType":"single protein","targets":["ENSG00000043591"]},
  {"actionType":"ANTAGONIST","chemblIds":["CHEMBL13","CHEMBL1200337","CHEMBL1200840","CHEMBL2062335","CHEMBL545787","CHEMBL3186768","CHEMBL2356097","CHEMBL3989566","CHEMBL1868892"],"mechanismOfAction":"Beta-1 adrenergic receptor antagonist","references":[{"ids":["setid=f3817543-544a-4afa-ac7c-c1b48c1307c2"],"source":"DailyMed","urls":["http://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=f3817543-544a-4afa-ac7c-c1b48c1307c2"]}],"targetName":"Beta-1 adrenergic receptor","targetType":"single protein","targets":["ENSG00000043591"]}]"""

  import sparkSession.implicits._

  val groupReferences: PrivateMethod[Dataset[Row]] =
    PrivateMethod[Dataset[Row]]('consolidateDuplicateReferences)

  "Mechanisms of action" should "not have duplicate MoA and cross references should be grouped" in {
    // given
    val df: DataFrame = sparkSession.read.json(Seq(testMoAInput).toDS())
    // when
    val results = MechanismOfAction invokePrivate groupReferences(df)
    // then
    assertResult(1)(results.count)
    assertResult(2, "Duplicate references should be removed")(
      results.select(sparkSize(col("references"))).head().get(0)
    )
  }

}
