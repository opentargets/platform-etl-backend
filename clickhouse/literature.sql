-- cat part-00* | clickhouse-client -h localhost --query="insert into ot.literature_log format JSONEachRow "
create database if not exists ot;
create table if not exists ot.literature_index
    engine = MergeTree()
        order by (keywordId, intHash64(pmid), year, month, day)
    as (
        select pmid, pmcid, keywordId, relevance, date, year, month, day
        from ot.literature_log
        );

create table if not exists ot.literature
    engine = MergeTree()
        order by (intHash64(pmid))
as (
    select pmid,
           any(pmcid) as pmcid,
           any(date) as date,
           any(year) as year,
           any(month) as month,
           any(day) as day,
           any(sentences) as sentences
    from ot.literature_log
    group by pmid
);
