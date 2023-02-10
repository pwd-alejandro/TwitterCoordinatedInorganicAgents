create table tweetcore.tweets_id_migration as
select t0.id,
       row_number() over (order by t0.id) as tweet_id_anon
from tweetcore.tweets t0
