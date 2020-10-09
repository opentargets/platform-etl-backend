create table if not exists ot.associations_otf_disease
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

create table if not exists ot.associations_otf_target
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
        lower(disease_data) as B_search,
        lower(target_data) as A_search
    from ot.associations_otf_log);
