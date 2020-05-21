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

#Check if the input is from gs bucket then it will strem the input file
echo $INPUT | grep gs://

if [ $? -eq 0 ]; then
  FILES="$(gsutil list $INPUT/*.json)"
  cmd="gsutil cp"
  trail="-"
else
  FILES="$(ls $INPUT/*.json)"
  cmd="cat"
  trail =""
fi

INDEXSETTING=1
printf "The index wont have an ID \n"
for f in $FILES
do
  if [ $INDEXSETTING -eq 1 ]; then
    echo $f
	`$cmd $f $trail | elasticsearch_loader --with-retry --es-host $ES --index-settings-file $INDEX_SETTINGS  --index $INDEX --type $TYPE_FIELD json --json-lines -  `
	INDEXSETTING=0
  else	  	
    echo $f
    `$cmd $f $trail | elasticsearch_loader --with-retry --es-host $ES --index $INDEX --type $TYPE_FIELD json --json-lines - `
  fi
done

