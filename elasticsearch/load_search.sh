#!/bin/bash
#cat "$1" | elasticsearch_loader --es-host "http://localhost:9200" --index-settings-file "index_settings_search.json" --with-retry --bulk-size 5000 --index searches --type search --id-field id json --json-lines -

export INDEX_SETTINGS="index_settings_search.json"
export RELEASE='20.04_'
export TYPE_FIELD="_doc"
export ES="http://localhost:9200"


export INDEX_NAME="search_target"
export INPUT="../out/search_targets"

./load_jsons_search.sh


export INDEX_NAME="search_drug"
export INPUT="../out/search_drugs"

./load_jsons_search.sh


export INDEX_NAME="search_disease"
export INPUT="../out/search_diseases"

./load_jsons_search.sh
