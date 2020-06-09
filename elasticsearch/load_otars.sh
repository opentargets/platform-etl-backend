#!/bin/bash

export INDEX_SETTINGS="index_settings.json"
export RELEASE=''
export INDEX_NAME="otar_projects"
export TYPE_FIELD="otar_projects"
export INPUT="../otar_projects"
# export INPUT="gs://ot-snapshots/etl/latest/eco"
export ES="http://localhost:9200"
export ID="efo_id"

./load_jsons.sh
