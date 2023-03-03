create table redacted_tables.english_tweets as
select *
from redacted_tables.tweets
where language = 'en'