package service

import cats.effect.testing.scalatest.AsyncIOSpec
import io.opentargets.workflow.model.{ClusterSettings, Configuration, Job}
import io.opentargets.workflow.service.{DataprocCluster, DataprocJobs}
import org.scalatest.AppendedClues
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

class DataprocClusterSpec extends AsyncFreeSpec with Matchers with AsyncIOSpec with AppendedClues {
  "An cluster should" - {
    val clusterSettings = ClusterSettings("cluster",
                                          "europe-west-1",
                                          "2.0-debian10",
                                          2000,
                                          "pd-ssd",
                                          "n1-highmem-64",
                                          4
    )
    "have 4 worker nodes" in {
      // given
      // when
      val cluster = DataprocCluster.createWorkflowTemplatePlacement.run(clusterSettings)
      val clusterConf = cluster.getManagedCluster.getConfig
      // then
      cluster.getManagedCluster.getConfig.hasMasterConfig shouldBe true withClue ("Cluster should have configured master config.")
      cluster.getManagedCluster.getConfig.hasWorkerConfig shouldBe true withClue ("Cluster should have configured worker config.")
    }
    "have 0 worker nodes" in {
      // given
      val conf = clusterSettings.copy(workerCount = 0)
      // when
      val cluster = DataprocCluster.createWorkflowTemplatePlacement.run(conf)
      val clusterConf = cluster.getManagedCluster.getConfig
      // then
      clusterConf.hasMasterConfig shouldBe true withClue ("Cluster should have configured master config.")
      clusterConf.hasWorkerConfig shouldBe true withClue ("Cluster should have configured worker config.")
      clusterConf.getWorkerConfig.getNumInstances shouldBe 0 withClue ("Cluster should have 0 workers.")
    }
  }
}
