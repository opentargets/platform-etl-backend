#!/bin/bash

export INDEX_SETTINGS="index_settings.json"
export RELEASE='20.04_'
export INDEX_NAME="target_relation"
export TYPE_FIELD="target_relation"
export INPUT="../out/targetRelation"
#export INPUT="gs://ot-snapshots/etl/latest/targetRelation"
export ES="http://localhost:9200"
export ID="id"

./load_jsons.sh
