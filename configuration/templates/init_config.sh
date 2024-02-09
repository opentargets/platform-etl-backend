#!/bin/bash

config_temp=$1
config_dest=$2

# Setup destination dir
cp $config_temp $config_dest
# Replace values
sed -i.bak \
    -e "s|<ENSEMBL_RELEASE_VERSION>|$OTOPS_ENSEMBL_RELEASE_VERSION|g" \
    -e "s|<CHEMBL_RELEASE_VERSION>|$OTOPS_CHEMBL_RELEASE_VERSION|g" \
    -e "s|<DATA_RELEASE_VERSION>|$OTOPS_DATA_RELEASE_VERSION|g" \
    -e "s|<GCS_ETL_CONFIG_PATH>|$OTOPS_GCS_ETL_CONFIG_PATH|g" \
    -e "s|<GCS_ETL_CONFIG_FILE>|$OTOPS_GCS_ETL_CONFIG_FILE|g" \
    -e "s|<GCS_ETL_JAR_PATH>|$OTOPS_GCS_ETL_JAR_PATH|g" \
    -e "s|<GCS_ETL_CONFIG_PATH_PPP>|$OTOPS_GCS_ETL_CONFIG_PATH_PPP|g" \
    -e "s|<GCS_ETL_CONFIG_FILE_PPP>|$OTOPS_GCS_ETL_CONFIG_FILE_PPP|g" \
    -e "s|<GCS_ETL_JAR_PATH_PPP>|$OTOPS_GCS_ETL_JAR_PATH_PPP|g" \
    -e "s|<ETL_JAR_FILE>|$ETL_JAR_FILENAME|g" \
    -- "${config_dest}" &&
    rm -- "${config_dest}.bak"