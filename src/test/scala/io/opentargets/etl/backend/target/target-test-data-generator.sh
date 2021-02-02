#!/bin/bash

# Paths to raw data files
HGNC=/data/hgnc_20.11.json

# convert to jsonl and take 500 random genes.
jq -c .response.docs[] $HGNC | shuf -n 500 > hgnc_test.jsonl