#!/bin/bash
#cat "$1" | elasticsearch_loader --es-host "http://localhost:9200" --index-settings-file "index_settings.json" --bulk-size 5000 --index cancerbiomarker --type cancerbiomarker json --json-lines -

export INDEX_NAME="evidences_aotf"
export INPUT="${PREFIX}/AOTFElasticsearch"

./load_jsons.sh
