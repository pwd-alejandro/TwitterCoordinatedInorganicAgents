create table redacted_tables.tweets_text_classification_post_test as

select t0.*
from redacted_tables.tweets_text_classification_post t0
left join redacted_tables.tweets_text_classification_post_train t1
on t0.user_id_anon = t1.user_id_anon
where t1.user_id_anon is null