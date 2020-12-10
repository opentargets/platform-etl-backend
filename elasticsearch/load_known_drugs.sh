#!/bin/bash
export INDEX_SETTINGS="index_settings_search_known_drugs.json"
export RELEASE=''
export INDEX_NAME="known_drugs"
export TYPE_FIELD="known_drugs"
export INPUT="../out/knownDrugs"
# export INPUT="gs://ot-snapshots/etl/latest/evidenceDrugDirect"
export ES="http://localhost:9200"


bash ./load_jsons.sh
