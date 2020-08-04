create database if not exists ot;
create table if not exists ot.aotf_direct_log(
    disease_id String,
    target_id String,
    datatype_id String,
    datasource_id String,
    datasource_harmonic Float64,
    datatype_harmonic Float64,
    disease_label String,
    target_name String,
    target_symbol String
) engine = Log;

create table if not exists ot.aotf_indirect_log(
    disease_id String,
    target_id String,
    datatype_id String,
    datasource_id String,
    datasource_harmonic Float64,
    datatype_harmonic Float64,
    disease_label String,
    target_name String,
    target_symbol String
) engine = Log;
