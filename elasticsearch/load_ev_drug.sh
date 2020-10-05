#!/bin/bash

#cat "$1" | elasticsearch_loader --es-host "http://localhost:9200" --index-settings-file "index_settings.json" --bulk-size 5000 --index targets --type target --id-field id json --json-lines -
export INDEX_SETTINGS="index_settings.json"
export RELEASE=''
export INDEX_NAME="ev_drug"
export TYPE_FIELD="ev_drug"
#export INPUT="../out/evidenceDrug"
export INPUT="gs://ot-snapshots/etl/latest/evidenceDrug"
export ES="http://localhost:9200"


./load_jsons.sh
