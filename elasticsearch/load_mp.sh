#!/bin/bash

export INDEX_SETTINGS="index_settings.json"
export RELEASE='20.04_'
export INDEX_NAME="mp"
export TYPE_FIELD="mp"
export INPUT="../luts/mp"
export ES="http://localhost:9200"
export ID="id"

./load_jsons.sh
