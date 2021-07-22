# where the ETL output prefix is located as inside it should find each step output
export PREFIX=${ETL_PREFIX:-"out"}
# empty prefix release. If you specify it it will be simply prefixed like "20.11_"...
export RELEASE=${ETL_RELEASE:-""}
# the default index settings file
export INDEX_SETTINGS=${ETL_INDEX_SETTINGS:-"index_settings.json"}
# default ES endpoint
export ES=${ETL_ES:-"http://localhost:9200"}

./load_cancerbiomarker.sh
./load_diseases.sh
./load_disease_hpo.sh
./load_drugs.sh
./load_eco.sh
./load_evidences.sh
./load_evidences_aotf.sh
./load_expression.sh
./load_hpo.sh
./load_interaction.sh
./load_interaction_evidence.sh
./load_mp.sh
./load_openfda_faers.sh
./load_otars.sh
./load_reactome.sh
./load_so.sh
./load_targets.sh

echo "INDEX_SETTINGS different"
./load_known_drugs.sh
./load_search.sh
