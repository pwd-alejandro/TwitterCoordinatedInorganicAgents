create table tweetcore.user_id_migration as
select t0.user_id,
       row_number() over (order by user_id) as user_id_anon
from tweetcore.users t0
