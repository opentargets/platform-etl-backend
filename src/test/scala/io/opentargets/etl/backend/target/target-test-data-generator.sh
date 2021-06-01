#!/bin/bash

# Paths to raw data files
HGNC=/data/hgnc_20.11.json
ENSEMBL=/data/homo_sapiens_core_101_38_genes.json
TRACTABILITY=/data/target-inputs/tractability/*.tsv

# convert to jsonl and take 500 random genes.
jq -c .response.docs[] $HGNC | shuf -n 500 >hgnc_test.jsonl
jq -c . $ENSEMBL | shuf -n 500 >ensembl_test.jsonl

head -n 51 $TRACTABILITY >>tractability_50.csv.gz

# Uniprot is in a flat text file. Records are separated by new line beginning with //
head -n 4013 uniprot_reviewed_homo.txt >>sample_10.txt
