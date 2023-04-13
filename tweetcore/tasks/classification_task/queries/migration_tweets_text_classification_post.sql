drop table redacted_tables.tweets_text_classification_post;

create table redacted_tables.tweets_text_classification_post as

select t0.user_id_anon,
       t0.agg_text,
       t0.number_tweets,
       case
           when t1.label = 'bot' then 1
           when t1.label = 'human' then 0
           else -1 end as target
from redacted_tables.tweets_text_classification_post_backup t0
         left join redacted_tables.labels t1
                   on t0.user_id_anon = t1.user_id_anon