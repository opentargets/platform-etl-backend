#!/bin/bash

export INDEX_SETTINGS="index_settings_search.json"

export INDEX_NAME="search_target"
export INPUT="${PREFIX}/search/targetIndex"
./load_jsons.sh


export INDEX_NAME="search_drug"
export INPUT="${PREFIX}/search/drugIndex"
./load_jsons.sh


export INDEX_NAME="search_disease"
export INPUT="${PREFIX}/search/diseaseIndex"
./load_jsons.sh
