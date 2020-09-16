-- create database if not exists ot;
-- create table if not exists ot.aotf_direct_d
-- engine = MergeTree()
-- order by (disease_id, target_id, datasource_harmonic, datasource_id)
-- primary key (disease_id)
-- as select
--     disease_id,
--     target_id,
--     datatype_id,
--     datasource_id,
--     datasource_harmonic,
--     datatype_harmonic,
--     disease_label,
--     target_name,
--     target_symbol
-- from ot.aotf_direct_log;
--
-- create database if not exists ot;
-- create table if not exists ot.aotf_indirect_d
-- engine = MergeTree()
-- order by (disease_id, target_id, datasource_harmonic, datasource_id)
-- primary key (disease_id)
-- as select
--     disease_id,
--     target_id,
--     datatype_id,
--     datasource_id,
--     datasource_harmonic,
--     datatype_harmonic,
--     disease_label,
--     target_name,
--     target_symbol
-- from ot.aotf_indirect_log;
--
-- create database if not exists ot;
-- create table if not exists ot.aotf_direct_t
-- engine = MergeTree()
-- order by (target_id, disease_id, datasource_harmonic, datasource_id)
-- primary key (target_id)
-- as select
--     disease_id,
--     target_id,
--     datatype_id,
--     datasource_id,
--     datasource_harmonic,
--     datatype_harmonic,
--     disease_label,
--     target_name,
--     target_symbol
-- from ot.aotf_direct_log;
--
-- create database if not exists ot;
-- create table if not exists ot.aotf_indirect_t
-- engine = MergeTree()
-- order by (target_id, disease_id, datasource_harmonic, datasource_id)
-- primary key (target_id)
-- as select
--     disease_id,
--     target_id,
--     datatype_id,
--     datasource_id,
--     datasource_harmonic,
--     datatype_harmonic,
--     disease_label,
--     target_name,
--     target_symbol
-- from ot.aotf_indirect_log;

create table if not exists ot.associations_otf_left
engine = MergeTree()
order by (A, B, datasource_id)
primary key (A)
as select
    row_id,
    A,
    B,
    datatype_id,
    datasource_id,
    row_score,
    A_search,
    B_search 
from (select 
        row_id,
        disease_id as A,
        target_id as B,
        datatype_id,
        datasource_id,
        row_score,
        lower(disease_data) as A_search,
        lower(target_data) as B_search
    from ot.associations_otf_log);

create table if not exists ot.associations_otf_right
engine = MergeTree()
order by (A, B, datasource_id)
primary key (A)
as select
    row_id,
    A,
    B,
    datatype_id,
    datasource_id,
    row_score,
    A_search,
    B_search
from (select 
        row_id,
        disease_id as B,
        target_id as A,
        datatype_id,
        datasource_id,
        row_score,
        lower(disease_search) as B_search,
        lower(target_search) as A_search
    from ot.associations_otf_log);
