#!/bin/bash

export INDEX_SETTINGS="index_settings.json"
export RELEASE='20.04_'
export INDEX_NAME="openfda_faers"
export TYPE_FIELD="faer"
export INPUT="../out/openfda"
#export INPUT="gs://ot-snapshots/jsonl/etl/200415000_20_04/openfda"
export ES="http://localhost:9200"
# export ID="id"

./load_jsons.sh
