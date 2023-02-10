create table tweetcore.user_id_mentions_migration as
select t0.user_mentions_ids,
       t1.user_id,
       row_number() over (order by null) + 1000000 as user_mention_anon
from (
         select distinct
                t.i as user_mentions_ids
         from tweetcore.tweets
                  cross join lateral unnest(nullif(user_mentions::varchar[], '{}')) as t(i)
     ) t0
left join tweetcore.user_id_migration t1
    on concat('u', t0.user_mentions_ids) = t1.user_id
where t1.user_id is null;
