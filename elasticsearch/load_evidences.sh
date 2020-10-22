#!/bin/bash
#cat "$1" | elasticsearch_loader --es-host "http://localhost:9200" --index-settings-file "index_settings.json" --bulk-size 5000 --index cancerbiomarker --type cancerbiomarker json --json-lines -

FOLDERS="sourceId=cancer_gene_census
sourceId=chembl
sourceId=clingen sourceId=crispr
sourceId=europepmc
sourceId=eva
sourceId=eva_somatic
sourceId=expression_atlas
sourceId=gene2phenotype
sourceId=genomics_england
sourceId=intogen
sourceId=ot_genetics_portal
sourceId=phenodigm
sourceId=phewas_catalog
sourceId=progeny
sourceId=reactome
sourceId=slapenrich
sourceId=sysbio
sourceId=uniprot_literature
sourceId=uniprot_somatic
"

FOLDER_PREFIX="../out/etl/20.09/mkarmona/json/processedEvidences"

for folder in $FOLDERS; do
  IFS='=' read -ra tokens <<< "$folder"

  token="evidence_datasource_${tokens[1]}"

  full_folder="${FOLDER_PREFIX}/${folder}/"
  export INDEX_SETTINGS="index_settings.json"
  export RELEASE=''
  export INDEX_NAME="${token}"
  export TYPE_FIELD="${token}"
  export INPUT="${full_folder}"
  #export INPUT="gs://ot-snapshots/etl/latest/cancerBiomarkers"
  export ES="http://localhost:9200"
  ./load_jsons.sh
done

