create table tweetcore.migration_development_200k as
with humans_100k as (
    select t0.id      as tweet_id,
           t0.text,
           t1.label,
           t1.user_id as author_id
    from tweetcore.test t0
             left join tweetcore.labels t1
                       on t0.author_id:: varchar = t1.user_id
    where t0.language = 'en'
      and t1.label = 'human'
    limit 100000
),

     bots_100k as (
         select t0.id      as tweet_id,
                t0.text,
                t1.label,
                t1.user_id as author_id
         from tweetcore.test t0
                  left join tweetcore.labels t1
                            on t0.author_id:: varchar = t1.user_id
         where t0.language = 'en'
           and t1.label = 'bot'
         limit 100000
     )

select row_number() over (order by tweet_id) as index,
       t0.*
from (
         select *
         from humans_100k
         union all
         select *
         from bots_100k
     ) t0
order by tweet_id