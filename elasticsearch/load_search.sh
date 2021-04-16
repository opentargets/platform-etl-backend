#!/bin/bash

export INDEX_SETTINGS="index_settings_search.json"

export INDEX_NAME="search_target"
export INPUT="${PREFIX}/searchTarget"
./load_jsons.sh


export INDEX_NAME="search_drug"
export INPUT="${PREFIX}/searchDrug"
./load_jsons.sh


export INDEX_NAME="search_disease"
export INPUT="${PREFIX}/searchDisease"
./load_jsons.sh
