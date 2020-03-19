#!/bin/bash

export INDEX_SETTINGS="index_settings.json"
export INDEX_NAME="20.02_disease_relation"
export TYPE_FIELD="disease_relation"
export INPUT="../out/diseaseRelation"
export ES="http://localhost:9200"
export ID="id"

./load_jsons.sh
