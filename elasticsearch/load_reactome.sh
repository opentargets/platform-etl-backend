#!/bin/bash

export INDEX_SETTINGS="index_settings.json"
export RELEASE='20.04_'
export INDEX_NAME="reactome"
export TYPE_FIELD="reactome"
export INPUT="../out/reactome"
export ES="http://localhost:9200"
export ID="id"

./load_jsons.sh