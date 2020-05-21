#!/bin/bash

export INDEX_SETTINGS="index_settings.json"
export RELEASE='20.04_'
export INDEX_NAME="expression"
export TYPE_FIELD="expression"
#export INPUT="../out/expression"
export INPUT="gs://ot-snapshots/etl/latest/expression"
export ES="http://localhost:9200"
export ID="id"

./load_jsons.sh