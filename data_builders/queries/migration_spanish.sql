create table tweetcore.spanish_tweets as
select *
from tweetcore.test
where language = 'es'