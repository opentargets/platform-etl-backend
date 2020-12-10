#!/bin/bash

#cat "$1" | elasticsearch_loader --es-host "http://localhost:9200" --index-settings-file "index_settings.json" --bulk-size 5000 --index targets --type target --id-field id json --json-lines -
export INDEX_NAME="target"
export INPUT="${PREFIX}/targets"
export ID="id"

./load_jsons.sh
