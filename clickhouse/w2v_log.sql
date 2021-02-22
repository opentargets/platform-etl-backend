-- cat part-00* | clickhouse-client -h localhost --query="insert into ot.ml_w2v_log format JSONEachRow "
create database if not exists ot;
create table if not exists ot.ml_w2v_log
(
    word   String,
    vector Array(Float64)
) engine = Log;
