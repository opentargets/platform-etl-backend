#cat "$1" | elasticsearch_loader --es-host "http://localhost:9200" --index-settings-file "index_settings.json" --bulk-size 5000 --index drugs --type drug --id-field id json --json-lines -

subindexes=('molecule' 'mechanismOfAction' 'indication' 'drugWarnings')
subindexesnames=('drug' 'mechanism_of_action' 'indication' 'drug_warnings')

len=${#subindexes[@]}
for (( i=0; i<$len; i++ )); do
  export INDEX_NAME=${subindexesnames[$i]}
  export INPUT="${PREFIX}/${subindexes[$i]}"

  ./load_jsons.sh
done
