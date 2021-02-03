#!/bin/bash

# Paths to raw data files
HGNC=/data/hgnc_20.11.json
ENSEMBL=/data/homo_sapiens_core_101_38_genes.json

# convert to jsonl and take 500 random genes.
jq -c .response.docs[] $HGNC | shuf -n 500 > hgnc_test.jsonl
jq -c . $ENSEMBL | shuf -n 500 > ensembl_test.jsonl