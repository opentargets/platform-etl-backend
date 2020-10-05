#!/bin/bash

export INDEX_SETTINGS="index_settings.json"
export RELEASE=''
export INDEX_NAME="mouse_phenotypes"
export TYPE_FIELD="mouse_phenotypes"
export INPUT="../out/mouse_phenotypes"
export ES="http://localhost:9200"
export ID="id"

./load_jsons.sh
