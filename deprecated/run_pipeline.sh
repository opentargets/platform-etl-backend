export JAVA_OPTS="-Xms1G -Xmx4G"
time amm platformDataBackend.sc \
    --drugFilename input/19.06_drug-data.json \
    --targetFilename input/19.06_gene-data.json \
    --diseaseFilename input/19.06_efo-data.json \
    --evidenceFilename input/19.06_evidence-data-100k.json \
    --outputPathPrefix out_test/
