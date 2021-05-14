create table if not exists ot.ml_w2v
    engine = MergeTree()
        order by (word)
        primary key (word)
as
select category,
       word,
       norm,
       vector
from (select category, word, norm, vector from ot.ml_w2v_log);
