select *
from (select *
      from redacted_tables.tweets_text_classification_post
      limit 10) t0
         left join (select 18749 as user_id_anon
                          union all
                          select 18804 as user_id_anon) t1
on t0.user_id_anon = t1.user_id_anon
where t1.user_id_anon is null



