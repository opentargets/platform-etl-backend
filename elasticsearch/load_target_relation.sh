#!/bin/bash

export INDEX_SETTINGS="index_settings.json"
export INDEX_NAME="20.02_target_relation"
export TYPE_FIELD="target_relation"
export INPUT="../out/targetRelation"
export ES="http://localhost:9200"
export ID="id"

./load_jsons.sh
