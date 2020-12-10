#!/bin/bash

#An example of command is
# cat "$1" | elasticsearch_loader --es-host "http://localhost:9200" --index-settings-file "index_settings.json" --bulk-size 5000 --index targets --type target --id-field id json --json-lines -
echo INDEX_SETTINGS = $INDEX_SETTINGS
echo ES = $ES
echo INPUT = $INPUT
echo INDEX_NAME = $INDEX_NAME
echo ID = $ID
echo RELEASE = $RELEASE

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
  trail=""
fi

echo create index "$ES/$INDEX" with settings file $INDEX_SETTINGS
curl -XPUT -H 'Content-Type: application/json' --data @$INDEX_SETTINGS $ES/$INDEX

for f in $FILES
  do
    echo $f
    if [[ -n "$ID" ]]; then
       printf "The index will have %s \n" "$ID"
      $cmd $f $trail | elasticsearch_loader --with-retry --es-host $ES --bulk-size 5000 --index $INDEX --id-field $ID json --json-lines -
    else
       printf "The index wont have an ID \n"
      $cmd "$f" $trail | elasticsearch_loader --with-retry --es-host $ES --bulk-size 5000 --index "$INDEX" json --json-lines -
    fi
done

