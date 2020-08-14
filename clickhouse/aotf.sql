create database if not exists ot;
create table if not exists ot.aotf_direct_d
engine = MergeTree()
order by (disease_id, target_id, datasource_harmonic, datasource_id)
primary key (disease_id)
as select
    disease_id,
    target_id,
    datatype_id,
    datasource_id,
    datasource_harmonic,
    datatype_harmonic,
    disease_label,
    target_name,
    target_symbol 
from ot.aotf_direct_log;

create database if not exists ot;
create table if not exists ot.aotf_indirect_d
engine = MergeTree()
order by (disease_id, target_id, datasource_harmonic, datasource_id)
primary key (disease_id)
as select
    disease_id,
    target_id,
    datatype_id,
    datasource_id,
    datasource_harmonic,
    datatype_harmonic,
    disease_label,
    target_name,
    target_symbol 
from ot.aotf_indirect_log;

create database if not exists ot;
create table if not exists ot.aotf_direct_t
engine = MergeTree()
order by (target_id, disease_id, datasource_harmonic, datasource_id)
primary key (target_id)
as select
    disease_id,
    target_id,
    datatype_id,
    datasource_id,
    datasource_harmonic,
    datatype_harmonic,
    disease_label,
    target_name,
    target_symbol 
from ot.aotf_direct_log;

create database if not exists ot;
create table if not exists ot.aotf_indirect_t
engine = MergeTree()
order by (target_id, disease_id, datasource_harmonic, datasource_id)
primary key (target_id)
as select
    disease_id,
    target_id,
    datatype_id,
    datasource_id,
    datasource_harmonic,
    datatype_harmonic,
    disease_label,
    target_name,
    target_symbol 
from ot.aotf_indirect_log;