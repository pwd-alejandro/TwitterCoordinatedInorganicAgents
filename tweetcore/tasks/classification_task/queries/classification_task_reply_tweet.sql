drop table redacted_tables.tweets_text_classification_reply_backup;

create table redacted_tables.tweets_text_classification_reply_backup as
select t0.user_id_anon,
       count(0) as number_tweets,
       string_agg(t0.text,E'\n') as agg_text
from (select user_id_anon,
             text,
             case
                                      when user_id_anon = in_reply_to_user_id_anon then 'thread'
                                      when type = 'post' and in_reply_to_user_id_anon is not null then 'replied_to'
                                      when type = 'post' and text like 'RT @%' then 'retweeted'
                                      else type end                                       as type_corrected
             from redacted_tables.tweets) t0
where t0.type_corrected = 'replied_to'
group by 1
