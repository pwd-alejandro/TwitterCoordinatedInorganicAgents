create table redacted_tables.labels as
select --t0.user_id,
       t1.user_id_anon,
       t0.label
from tweetcore.labels t0
         left join tweetcore.user_id_migration t1
                   on concat('u', t0.user_id) = t1.user_id