--create table redacted_tables.users as
with user_mentions_ids as (
    select t0.tweet_id,
           t0.user_mentions,
           array_agg(coalesce(t1.user_id_anon, t2.user_mention_anon)) as user_mentions_anon
    from (
             select id  as tweet_id,
                    user_mentions,
                    t.i as user_mentions_ids
             from tweetcore.tweets
                      cross join lateral unnest(nullif(user_mentions::varchar[], '{}')) as t(i)
         ) t0
             left join tweetcore.user_id_migration t1
                       on concat('u', t0.user_mentions_ids) = t1.user_id
             left join tweetcore.user_id_mentions_migration t2
                       on t0.user_mentions_ids = t2.user_mentions_ids
    group by 1, 2
),




select id,
       user_mentions
from tweetcore.tweets
where id = 't1103321794646368256'

select *
from tweetcore.user_id_mentions_migration
where user_mentions_ids = '1035364341602758657'

select t1.tweet_id_anon,
       t2.user_id_anon,
       --t0.author_id,
       t0.created_at,
       t0.type,
       t0.text,
       t0.language,
       t0.geo_location,
       t0.referenced_tweets_types,
       t0.referenced_tweets_ids,
       t0.user_mentions,
       t0.media_types,
       t3.user_id_anon as in_reply_to_user_id_anon,
       --t0.in_reply_to_user_id,
       t0.possibly_sensitive
from (select * from tweetcore.tweets limit 10) t0
         left join tweetcore.tweets_id_migration t1
                   on t0.id = t1.id
         left join tweetcore.user_id_migration t2
                   on concat('u', t0.author_id::varchar) = t2.user_id::varchar
         left join tweetcore.user_id_migration t3
                   on concat('u', t0.in_reply_to_user_id::varchar) = t3.user_id::varchar


select t0.id,
       t0.author_id,
       t0.text,
       t0.user_mentions_list,
       array_agg(t1.user_id_anon)
from (
         select id,
                author_id,
                text,
                user_mentions as user_mentions_list,
                t.i           as user_mentions
         from tweetcore.tweets
                  cross join lateral unnest(coalesce(nullif(user_mentions::varchar[], '{}'),
                                                     array [null::varchar])) as t(i)
         limit 10) t0
         left join tweetcore.user_id_migration t1
                   on concat('u', t0.user_mentions::varchar) = t1.user_id
group by 1, 2, 3, 4;


select *
from tweetcore.user_id_migration
where user_id = 'u1152345044512382976'

select *
from tweetcore.tweets
where id::varchar = 't1458183268763934721'


select nullif('{}'::varchar[], '{}')

select coalesce(nullif('{}'::varchar[], '{}'), '{12}'::varchar[])

select id, t.i
from the_table
         cross join lateral unnest(coalesce(nullif(int_values, '{}'), array [null::int])) as t(i);
