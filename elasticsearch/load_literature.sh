#!/bin/bash

export INDEX_NAME="literature"
export INPUT="${PREFIX}/literature/literatureIndex"
export ID="pmid"

./load_jsons.sh
