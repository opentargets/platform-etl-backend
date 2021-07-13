-- create database if not exists ot;
-- create table if not exists ot.aotf_direct_log(
--     disease_id String,
--     target_id String,
--     datatype_id String,
--     datasource_id String,
--     datasource_harmonic Float64,
--     datatype_harmonic Float64,
--     disease_label String,
--     target_name String,
--     target_symbol String
-- ) engine = Log;
--
-- create table if not exists ot.aotf_indirect_log(
--     disease_id String,
--     target_id String,
--     datatype_id String,
--     datasource_id String,
--     datasource_harmonic Float64,
--     datatype_harmonic Float64,
--     disease_label String,
--     target_name String,
--     target_symbol String
-- ) engine = Log;

-- cat part-00* | clickhouse-client -h localhost --query="insert into ot.associations_otf2_log format JSONEachRow "
-- cat part-00* | elasticsearch_loader --es-host http://es7-20-06:9200 --index-settings-file=../index.json --index evidences_aotf --timeout 120 --with-retry --id-field row_id --delete  json --json-lines -

-- {
--   "target_id": "ENSG00000005844",
--   "disease_id": "EFO_0000565",
--   "row_id": "6af5fc46f3953a178540e0768e006f4e7de72eb9",
--   "datasource_id": "europepmc",
--   "datatype_id": "literature",
--   "row_score": 0.14,
--   "disease_data": "EFO_0000565 leukemia",
--   "name": "leukemia",
--   "facet_therapeuticAreas": [
--     "hematologic disease",
--     "musculoskeletal or connective tissue disease",
--     "cell proliferation disorder",
--     "immune system disease"
--   ],
--   "target_data": "ENSG00000005844 integrin subunit alpha L ITGAL",
--   "facet_classes": [
--     {
--       "l1": "Adhesion"
--     },
--     {
--       "l1": "Membrane receptor"
--     }
--   ],
--   "facet_tractability_antibody": [
--     "clinical_precedence",
--     "predicted_tractable_high_confidence",
--     "predicted_tractable_med_low_confidence"
--   ],
--   "facet_tractability_smallmolecule": [
--     "clinical_precedence",
--     "discovery_precedence",
--     "predicted_tractable"
--   ],
--   "facet_reactome": [
--     {
--       "l1": "Immune System",
--       "l2": "Immunoregulatory interactions between a Lymphoid and a non-Lymphoid cell"
--     },
--     {
--       "l1": "Hemostasis"
--     },
--     {
--       "l1": "Extracellular matrix organization"
--     },
--     {
--       "l1": "Immune System",
--       "l2": "Neutrophil degranulation"
--     },
--     {
--       "l1": "Gene expression (Transcription)",
--       "l2": "Generic Transcription Pathway"
--     }
--   ]
-- }

set input_format_skip_unknown_fields=1;
set input_format_import_nested_json=1;

create table if not exists ot.associations_otf2_log(
    row_id String,
    disease_id String,
    target_id String,
    disease_data Nullable(String),
    target_data Nullable(String),
    datasource_id String,
    datatype_id String,
    row_score Float64,
    facet_therapeuticAreas Nullable(Array(String)) default [],
    facet_classes Nested(
        l1 String,
        l2 String
        )
) engine = Log;
