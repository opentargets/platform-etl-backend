#!/bin/bash

#An example of command is
# cat "$1" | elasticsearch_loader --es-host "http://localhost:9200" --index-settings-file "index_settings.json" --bulk-size 5000 --index targets --type target --id-field id json --json-lines -
echo $INDEX_SETTINGS
echo $ES
echo $INPUT
echo $INDEX_NAME
echo $TYPE_FIELD
echo $ID
echo $RELEASE

INDEX=$RELEASE$INDEX_NAME

FILES="$(ls $INPUT/*.json)"
for f in $FILES
do
  echo $f
  if [[ -n "$ID" ]]; then
    printf "The index will have %s \n" "$ID"
    cat  $f | elasticsearch_loader --with-retry --es-host $ES --index-settings-file $INDEX_SETTINGS --bulk-size 5000 --index $INDEX --type $TYPE_FIELD --id-field id json --json-lines - 
  else
    printf "The index wont have an ID \n"
    cat  $f | elasticsearch_loader --with-retry --es-host $ES --index-settings-file $INDEX_SETTINGS --bulk-size 5000 --index $INDEX --type $TYPE_FIELD json --json-lines -  
  fi
done

