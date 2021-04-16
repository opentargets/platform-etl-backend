#!/bin/bash
#cat "$1" | elasticsearch_loader --es-host "http://localhost:9200" --index-settings-file "index_settings.json" --bulk-size 5000 --index cancerbiomarker --type cancerbiomarker json --json-lines -

export INDEX_NAME="interaction_evidence"
export INPUT="${PREFIX}/interactionEvidence"

./load_jsons.sh
