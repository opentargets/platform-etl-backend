#!/bin/bash

subindexes=('molecule' 'mechanismOfAction' 'indication' 'drugWarnings')
subindexesnames=('drug' 'mechanism_of_action' 'indication' 'drug_warnings')

len=${#subindexes[@]}
for (( i=0; i<$len; i++ )); do
  export INDEX_NAME=${subindexesnames[$i]}
  export INPUT="${PREFIX}/${subindexes[$i]}"

  ./load_jsons.sh
done
