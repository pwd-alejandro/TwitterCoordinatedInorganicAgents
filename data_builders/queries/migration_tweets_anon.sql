--create table select count(0) from tweetcore.tweets as
with user_mentions_ids as (
    select t0.tweet_id,
           t0.user_mentions,
           array_agg(coalesce(t1.user_id_anon, t2.user_mention_anon)) as user_mentions_anon
    from (
             select id  as tweet_id,
                    user_mentions,
                    t.i as user_mentions_ids
             from (select * from tweetcore.tweets where user_mentions <> '{}') t0
                      cross join lateral unnest(user_mentions::varchar[]) as t(i)
         ) t0
             left join tweetcore.user_id_migration t1
                       on concat('u', t0.user_mentions_ids) = t1.user_id
             left join tweetcore.user_id_mentions_migration t2
                       on t0.user_mentions_ids = t2.user_mentions_ids
    group by 1, 2
),

referenced_tweets_ids as (
         select t0.tweet_id,
                t0.referenced_tweets_ids,
                array_agg(coalesce(t1.tweet_id_anon, t2.referenced_tweet_id_anon)) as referenced_tweets_ids_anon
         from (
                  select id  as tweet_id,
                         referenced_tweets_ids,
                         t.i as ref_tweets_ids
                  from (select * from tweetcore.tweets where referenced_tweets_ids <> '{}') t00
                           cross join lateral unnest(referenced_tweets_ids::varchar[]) as t(i)
              ) t0
                  left join tweetcore.tweets_id_migration t1
                            on concat('t', t0.ref_tweets_ids) = t1.id
              left join tweetcore.referenced_tweet_id_migration t2
                        on t0.ref_tweets_ids = t2.ref_tweets_ids
         group by 1, 2
     )

select --t0.id,
       t1.tweet_id_anon,
       --t0.author_id,
       t2.user_id_anon,
       t0.created_at,
       t0.type,
       t0.text,
       t0.language,
       t0.geo_location,
       t0.referenced_tweets_types,
       --t0.referenced_tweets_ids,
       coalesce(t5.referenced_tweets_ids_anon, '{}') as referenced_tweets_ids_anon,
       --t0.user_mentions,
       coalesce(t4.user_mentions_anon, '{}')         as user_mentions_anon,
       t0.media_types,
       --t0.in_reply_to_user_id,
       t3.user_id_anon                               as in_reply_to_user_id_anon,
       t0.possibly_sensitive
from (select * from tweetcore.tweets) t0
         left join tweetcore.tweets_id_migration t1
                   on t0.id = t1.id
         left join tweetcore.user_id_migration t2
                   on concat('u', t0.author_id::varchar) = t2.user_id::varchar
         left join tweetcore.user_id_migration t3
                   on concat('u', t0.in_reply_to_user_id::varchar) = t3.user_id::varchar
         left join user_mentions_ids t4
                   on t0.id = t4.tweet_id
         left join referenced_tweets_ids t5
                   on t0.id = t5.tweet_id
