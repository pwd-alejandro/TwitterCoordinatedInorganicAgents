create table tweetcore.english_tweets as
select *
from tweetcore.test
where language = 'en'