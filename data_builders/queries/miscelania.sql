select c,
       count(0)
from (
select author_id,
       count(0) as c
from tweetcore.tweets
group by 1) t0
group by 1


select count(distinct author_id)
from tweetcore.tweets
where language in ('en', 'und')
