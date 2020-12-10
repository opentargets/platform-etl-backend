#cat "$1" | elasticsearch_loader --es-host "http://localhost:9200" --index-settings-file "index_settings.json" --bulk-size 5000 --index drugs --type drug --id-field id json --json-lines -

export INDEX_NAME="drug"
export INPUT="${PREFIX}/drugs"
export ID="id"

./load_jsons.sh
