#!/bin/bash

worker_list="32"
for work_run in $worker_list; do
    machine_cores=$work_run
    workers=0
    job_prefix=$(uuidgen -r)                                                                                                                                 
    cluster_name="etl-backend-carmona-${machine_cores}-${workers}"
    job_name="etl-backend-carmona-test-${job_prefix}-${machine_cores}-${workers}"
    job_conf="etl-backend-carmona-test-${machine_cores}-${workers}.conf"
    job_jar=gs://ot-snapshots/carmona/io-opentargets-etl-backend-assembly-0.3.5.jar

cat <<EOF > "${job_conf}"
common {
  default-steps = [
    "association"
  ]

  output-format = "json"
  output = "gs://ot-snapshots/carmona/etl-200706"
  inputs {
    target {
      format ="parquet"
      path = "gs://ot-snapshots/parquet/20.06/parquet/gene_parquet"
    }
    disease  {
      format ="parquet"
      path = "gs://ot-snapshots/parquet/20.06/parquet/efo_parquet"
    }
    drug  {
      format ="parquet"
      path = "gs://ot-snapshots/parquet/20.06/parquet/drug_parquet"
    }
    evidence  {
      format ="parquet"
      path = "gs://ot-snapshots/parquet/20.06/parquet/evidence_parquet"
    }
    associations  {
      format ="parquet"
      path = "gs://ot-snapshots/parquet/20.06/parquet/association_parquet"
    }
    ddr  {
      format ="parquet"
      path = "gs://ot-snapshots/parquet/20.06/parquet/relation_parquet"
    }
    reactome {
      format ="parquet"
      path = "gs://ot-snapshots/parquet/20.06/parquet/rea_parquet"
    }
    eco  {
      format ="parquet"
      path = "gs://ot-snapshots/parquet/20.06/parquet/eco_parquet"
    }
    expression  {
      format ="parquet"
      path = "gs://ot-snapshots/parquet/20.06/parquet/expression_parquet/"
    }
   tep {
      format ="json"
      path = "gs://ot-snapshots/jsonl/20.06/tep-2020-06-01.json"
   }
   mousephenotypes {
      format ="json"
      path = "gs://ot-snapshots/jsonl/20.06/parquet/20.06/parquet_mousephenotypes.json"
   }
  }
}

EOF

    # --properties=yarn:yarn.nodemanager.vmem-check-enabled=false,spark:spark.debug.maxToStringFields=1024,spark:spark.master=yarn \
    echo scaling test machine type $machine_cores with $workers workers
    (gcloud beta dataproc clusters create \
        $cluster_name \
        --image-version=1.5-debian10 \
        --single-node \
        --master-machine-type=n1-highmem-$machine_cores \
        --master-boot-disk-size=1000 \
        --zone=europe-west1-d \
        --project=open-targets-eu-dev \
        --region=europe-west1 \
        --initialization-action-timeout=20m \
        --max-idle=10m && \
            gcloud dataproc jobs submit spark \
                --id=$job_name \
                --cluster=$cluster_name \
                --project=open-targets-eu-dev \
                --region=europe-west1 \
                --async \
                --files=$job_conf \
                --properties=spark.executor.extraJavaOptions=-Dconfig.file=$job_conf,spark.driver.extraJavaOptions=-Dconfig.file=$job_conf \
                --jar=$job_jar) &
done
wait