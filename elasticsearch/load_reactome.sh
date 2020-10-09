#!/bin/bash

export INDEX_SETTINGS="index_settings.json"
export RELEASE=''
export INDEX_NAME="reactome"
export TYPE_FIELD="reactome"
export INPUT="../out/reactome"
#export INPUT="gs://ot-snapshots/etl/latest/reactome"
export ES="http://localhost:9200"
export ID="id"

./load_jsons.sh
