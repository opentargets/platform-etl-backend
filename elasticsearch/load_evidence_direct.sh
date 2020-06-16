#!/bin/bash
export INDEX_SETTINGS="index_settings_search_evidence_drug_direct.json"
export RELEASE=''
export INDEX_NAME="evidence_drug_direct"
export TYPE_FIELD="evidence_drug_direct"
export INPUT="../out/evidenceDrugDirect"
# export INPUT="gs://ot-snapshots/etl/latest/evidenceDrugDirect"
export ES="http://localhost:9200"


bash ./load_jsons.sh
