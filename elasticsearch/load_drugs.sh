#cat "$1" | elasticsearch_loader --es-host "http://localhost:9200" --index-settings-file "index_settings.json" --bulk-size 5000 --index drugs --type drug --id-field id json --json-lines -

export INDEX_SETTINGS="index_settings.json"
export RELEASE='20.04_'
export INDEX_NAME="drug"
export TYPE_FIELD="drug"
export INPUT="../out/drugs"
export ES="http://localhost:9200"
export ID="id"

./load_jsons.sh
