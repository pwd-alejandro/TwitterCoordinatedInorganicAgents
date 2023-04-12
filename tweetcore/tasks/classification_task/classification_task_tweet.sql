create table redacted_tables.tweets_text_classification_backup as
select user_id_anon,
       string_agg(text,E'\n') as agg_text
from redacted_tables.tweets
group by 1
