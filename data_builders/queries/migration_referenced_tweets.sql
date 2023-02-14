create table tweetcore.referenced_tweet_id_migration as
select t0.ref_tweets_ids,
       t1.id,
       row_number() over (order by null) + 88217457 as referenced_tweet_id_anon
from (
         select distinct t.i as ref_tweets_ids
         from (select * from tweetcore.tweets where referenced_tweets_ids <> '{}') t0
                  cross join lateral unnest(referenced_tweets_ids::varchar[]) as t(i)
     ) t0
         left join tweetcore.tweets_id_migration t1
                   on concat('t', t0.ref_tweets_ids) = t1.id
where t1.id is null
