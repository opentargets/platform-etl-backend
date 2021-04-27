-- cat part-00* | clickhouse-client -h localhost --query="insert into ot.literature_log format JSONEachRow "
create database if not exists ot;
create table if not exists ot.literature_log(
    pmid UInt64,
    pmcid Nullable(String),
    date Date,
    year UInt16,
    month UInt8,
    day UInt8,
    keywordId String,
    relevance Float64,
    keywordType FixedString(2),
    sentences Nullable(String)
) engine = Log;