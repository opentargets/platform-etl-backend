#!/bin/bash

export INDEX_SETTINGS="index_settings.json"
export INDEX_NAME="20.02_reactome"
export TYPE_FIELD="reactome"
export INPUT="../out/reactome"
export ES="http://localhost:9200"
export ID="id"

./load_jsons.sh