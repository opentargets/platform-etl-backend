create table if not exists ot.ml_w2v
    engine = MergeTree()
        order by (word)
        primary key (word)
as
select multiIf(startsWith(word, 'ENSG'), 'target', startsWith(word, 'CHEMBL'), 'drug', 'disease') as category,
       word,
       vector
from (select word, vector from ot.ml_w2v_log);
