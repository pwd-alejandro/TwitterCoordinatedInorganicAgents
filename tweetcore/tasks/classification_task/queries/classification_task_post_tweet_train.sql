create table redacted_tables.tweets_text_classification_post_train as

select *
from redacted_tables.tweets_text_classification_post t0
order by random()
limit 627454