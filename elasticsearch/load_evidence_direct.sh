#!/bin/bash
export INDEX_SETTINGS="index_settings.json"
export INDEX_NAME="20.02_direct_evidence"
export TYPE_FIELD="ev_direct"
export INPUT="../out/evidenceDrugDirect"
export ES="http://localhost:9200"


./load_jsons.sh