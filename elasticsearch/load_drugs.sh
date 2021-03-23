#cat "$1" | elasticsearch_loader --es-host "http://localhost:9200" --index-settings-file "index_settings.json" --bulk-size 5000 --index drugs --type drug --id-field id json --json-lines -

subindexes=('molecule' 'mechanismOfAction' 'indication' 'drugWarnings')

for si in "${subindexes[@]}"; do
  export INDEX_NAME=$si
  export INPUT="${PREFIX}/$si"

  ./load_jsons.sh
done
