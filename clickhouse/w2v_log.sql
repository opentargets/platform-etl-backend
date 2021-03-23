-- gsutil -m cat gs://ot-snapshots/etl/outputs/21.03.1/literature/vectors/part\* | clickhouse-client -h localhost --query="insert into ot.ml_w2v_log format JSONEachRow "
create database if not exists ot;
create table if not exists ot.ml_w2v_log
(
    category String,
    word   String,
    norm    Float64,
    vector Array(Float64)
) engine = Log;
