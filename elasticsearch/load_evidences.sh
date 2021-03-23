#!/bin/bash
#cat "$1" | elasticsearch_loader --es-host "http://localhost:9200" --index-settings-file "index_settings.json" --bulk-size 5000 --index cancerbiomarker --type cancerbiomarker json --json-lines -

FOLDER_PREFIX="${PREFIX}/evidence"
FOLDERS=$(ls -1 $FOLDER_PREFIX | grep 'sourceId')

for folder in $FOLDERS; do
  IFS='=' read -ra tokens <<< "$folder"

  token="evidence_datasource_${tokens[1]}"

  full_folder="${FOLDER_PREFIX}/${folder}/"

  export ID='id'
  export INDEX_NAME="${token}"
  export INPUT="${full_folder}"
  ./load_jsons.sh
done

