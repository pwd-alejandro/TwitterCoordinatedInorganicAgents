create table tweetcore.english_tweets as
select *
from tweetcore.tweets
where language = 'en'