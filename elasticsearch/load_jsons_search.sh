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
INDEXSETTING=1
printf "The index wont have an ID \n"
for f in $FILES
do
  if [ $INDEXSETTING -eq 1 ]; then
    echo $f
	cat  $f | elasticsearch_loader --es-host $ES --index-settings-file $INDEX_SETTINGS --bulk-size 5000 --index $INDEX --type $TYPE_FIELD json --json-lines -  
	INDEXSETTING=0
  else	  	
    echo $f
    cat  $f | elasticsearch_loader --es-host $ES --bulk-size 5000 --index $INDEX --type $TYPE_FIELD json --json-lines -  
  fi
done

