#!/bin/bash

export INDEX_SETTINGS="index_settings.json"
export RELEASE=''
export INDEX_NAME="eco"
export TYPE_FIELD="eco"
export INPUT="../out/eco"
#export INPUT="gs://ot-snapshots/etl/latest/eco"
export ES="http://localhost:9200"
export ID="id"

./load_jsons.sh
