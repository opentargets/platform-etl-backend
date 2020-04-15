#!/bin/bash

export INDEX_SETTINGS="index_settings.json"
export RELEASE='20.04_'
export INDEX_NAME="disease_relation"
export TYPE_FIELD="disease_relation"
export INPUT="../out/diseaseRelation"
export ES="http://localhost:9200"
export ID="id"

./load_jsons.sh
