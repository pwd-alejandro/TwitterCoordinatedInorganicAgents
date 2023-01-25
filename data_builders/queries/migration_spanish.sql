create table tweetcore.spanish_tweets as
select *
from tweetcore.tweets
where language = 'es'