create table tweetcore.migration_development as
with humans_500k as (
    select t0.id      as tweet_id,
           t0.text,
           t1.label,
           t1.user_id as author_id
    from tweetcore.english_tweets t0
             left join tweetcore.labels t1
                       on t0.author_id:: varchar = t1.user_id
    where t1.label = 'human'
    order by random()
    limit 500000
),

     bots_100k as (
         select t0.id      as tweet_id,
                t0.text,
                t1.label,
                t1.user_id as author_id
         from tweetcore.english_tweets t0
                  left join tweetcore.labels t1
                            on t0.author_id:: varchar = t1.user_id
         where t1.label = 'bot'
         order by random()
         limit 100000
     )

select row_number() over (order by tweet_id) -1 as index,
       t0.*
from (
         select *
         from humans_500k
         union all
         select *
         from bots_100k
     ) t0
order by tweet_id