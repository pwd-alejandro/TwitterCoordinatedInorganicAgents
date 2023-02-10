create table redacted_tables.users as
select t1.user_id_anon,
       --t0.user_id,
       t0.joined_twitter,
       --t0.username,
       t0.username_length,
       t0.username_words,
       t0.username_numbers,
       t0.username_special_char,
       t0.username_emojis,
       t0.username_capital_letters,
       --t0.name,
       t0.name_length,
       t0.name_words,
       t0.name_numbers,
       t0.name_special_char,
       t0.name_emojis,
       t0.name_capital_letters,
       t0.description,
       t0.description_cashtags,
       t0.description_hashtags,
       t0.description_mentions,
       t0.description_urls,
       t0.location,
       case
           when t0.pinned_tweet_id is not null then True
           else False end as has_pinned_tweet_id,
       t0.default_profile_picture,
       t0.followers_count,
       t0.following_count,
       t0.tweet_count,
       t0.listed_count,
       t0.url,
       t0.withheld,
       t0.protected,
       t0.verified
from tweetcore.users t0
         left join tweetcore.user_id_migration t1
                   on t0.user_id = t1.user_id
