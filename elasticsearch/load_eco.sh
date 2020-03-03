#!/bin/bash

export INDEX_SETTINGS="index_settings.json"
export INDEX_NAME="20.02_eco"
export TYPE_FIELD="eco"
export INPUT="../out/eco"
export ES="http://localhost:9200"
export ID="id"

./load_jsons.sh