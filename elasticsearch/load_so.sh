#!/bin/bash

export INDEX_SETTINGS="index_settings.json"
export RELEASE=''
export INDEX_NAME="so"
export TYPE_FIELD="so"
export INPUT="/home/mkarmona/src/opentargets/data/SO-Ontologies-3.1/"
#export INPUT="gs://ot-snapshots/etl/latest/eco"
export ES="http://localhost:9200"
export ID="id"

./load_jsons.sh
