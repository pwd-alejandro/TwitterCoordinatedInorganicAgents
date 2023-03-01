create table redacted_tables.features_user_classification as
with user_features as (
    select user_id_anon,
           -- datetime joined
           extract('year' from joined_twitter::timestamp)                          as year_joined,
           extract('month' from joined_twitter::timestamp)                         as month_joined,
           extract('day' from joined_twitter::timestamp)                           as day_of_month_joined,
           extract(isodow from joined_twitter::timestamp)                          as day_of_week_joined,
           case
               when extract(isodow from joined_twitter::timestamp) in (6, 7) then 1
               else 0 end                                                          as joined_weekend,
           extract('hour' from joined_twitter::timestamp)                          as hour_joined,
           -- username
           username_length                                                         as username_length,
           username_numbers / nullif(username_length, 0)::double precision         as fraction_numbers_username,
           username_emojis / nullif(username_length, 0)::double precision          as fraction_emojis_username,
           username_capital_letters / nullif(username_length, 0)::double precision as fraction_capital_letters_username,
           username_special_char / nullif(username_length, 0)::double precision    as fraction_special_char_username,
           username_words                                                          as username_words,
           username_length / nullif(username_words, 0)::double precision           as avg_word_length_username,
           -- name
           name_length                                                             as name_length,
           name_numbers / nullif(name_length, 0)::double precision                 as fraction_numbers_name,
           name_emojis / nullif(name_length, 0)::double precision                  as fraction_emojis_name,
           name_capital_letters / nullif(name_length, 0)::double precision         as fraction_capital_letters_name,
           name_special_char / nullif(name_length, 0)::double precision            as fraction_special_char_name,
           name_words                                                              as name_words,
           name_length / nullif(name_words, 0)::double precision                   as avg_word_length_name,
           -- description
           coalesce(array_length(description_cashtags::varchar[], 1), 0) +
           coalesce(array_length(description_hashtags::varchar[], 1), 0)           as description_tags,
           coalesce(array_length(description_mentions::varchar[], 1), 0)           as description_mentions,
           coalesce(array_length(description_urls::varchar[], 1), 0)               as description_urls,
           -- other
           case when location is not null then 1 else 0 end                        as has_location,
           has_pinned_tweet_id::int                                                as has_pinned_tweet,
           default_profile_picture::int                                            as has_default_profile_picture,
           following_count                                                         as following_count,
           following_count / nullif(followers_count, 0)::double precision          as following_followers_ratio,
           tweet_count                                                             as tweet_count,
           listed_count / nullif(tweet_count, 0)::double precision                 as listed_tweet_ratio,
           tweet_count / nullif(followers_count, 0)::double precision              as tweet_followers_ratio,
           coalesce(array_length(withheld::varchar[], 1), 0)                       as number_countries_withheld,
           protected::int                                                          as is_protected,
           -- what does 'verified' mean now?
           verified::int                                                           as is_verified


    from redacted_tables.users
),

     mentions_features as (
         select user_id_anon                                                                       as user_id_anon,
                total_number_mentions                                                              as total_number_mentions,
                number_tweets                                                                      as number_tweets_sample,
                number_tweets_with_mention                                                         as number_tweets_with_mention,
                unique_number_mentions                                                             as unique_number_mentions,
                case
                    when number_tweets = 0 then -1
                    else total_number_mentions / number_tweets::double precision end               as avg_number_mentions_tweet,
                case
                    when number_tweets_with_mention = 0 then -1
                    else total_number_mentions / number_tweets_with_mention:: double precision end as avg_number_mentions_tweet_with_mention,
                case
                    when number_tweets = 0 then -1
                    else unique_number_mentions / number_tweets::double precision end              as avg_number_unique_mentions_tweet,
                case
                    when number_tweets_with_mention = 0 then -1
                    else unique_number_mentions / number_tweets_with_mention::double precision end as avg_number_unique_mentions_tweet_with_mention,
                case
                    when number_tweets = 0 then -1
                    else number_tweets_with_mention / number_tweets::double precision end          as rate_tweets_with_mention,
                case
                    when total_number_mentions = 0 then -1
                    else unique_number_mentions / total_number_mentions::double precision end      as rate_unique_total_mentions,
                max_number_mentions_tweet                                                          as max_number_mentions_tweet

         from (
                  select user_id_anon                           as user_id_anon,
                         total_number_mentions                  as total_number_mentions,
                         count(distinct case
                                            when user_mentions_ids_anon is not null then tweet_id_anon
                                            else null end)      as number_tweets_with_mention,
                         count(distinct tweet_id_anon)          as number_tweets,
                         count(distinct user_mentions_ids_anon) as unique_number_mentions,
                         max(number_mentions_in_tweet)          as max_number_mentions_tweet
                  from (
                           select user_id_anon                                    as user_id_anon,
                                  tweet_id_anon                                   as tweet_id_anon,
                                  user_mentions_ids_anon                          as user_mentions_ids_anon,
                                  sum(case when user_mentions_ids_anon is not null then 1 else 0 end)
                                  over (partition by user_id_anon)                as total_number_mentions,
                                  sum(case when user_mentions_ids_anon is not null then 1 else 0 end)
                                  over (partition by user_id_anon, tweet_id_anon) as number_mentions_in_tweet
                           from (
                                    select user_id_anon,
                                           tweet_id_anon,
                                           user_mentions_anon,
                                           t.i as user_mentions_ids_anon
                                    from redacted_tables.tweets
                                             cross join lateral unnest(coalesce(
                                            nullif(user_mentions_anon::varchar[], '{}'),
                                            array [null::int]::varchar[])) as t(i)
                                ) t0
                       ) t0
                  group by 1, 2
              ) t0
     ),

     core_activity_features as (
         select user_id_anon                                                          as user_id_anon,
                mode_month_activity                                                   as mode_month_activity,
                mode_day_activity                                                     as mode_day_activity,
                mode_day_of_week_activity                                             as mode_day_of_week_activity,
                mode_hour_activity                                                    as mode_hour_activity,
                mode_type_corrected                                                   as mode_type_corrected,
                mode_language                                                         as mode_language,
                number_languages                                                      as number_languages,
                number_tweets                                                         as core_number_tweets,
                less_used_language                                                    as less_used_language,
                max_actv_freq_minutes                                                 as max_actv_freq_minutes,
                min_actv_freq_minutes                                                 as min_actv_freq_minutes,
                avg_actv_freq_minutes                                                 as avg_actv_freq_minutes,
                median_actv_freq_minutes                                              as median_actv_freq_minutes,
                case
                    when number_tweets = 0 then -1
                    else
                        count_sensitive / number_tweets::double precision end         as rate_sensitive_tweets,
                case
                    when number_tweets = 0 then -1
                    else
                        count_media / number_tweets::double precision end             as rate_media_tweets,
                case
                    when number_tweets = 0 then -1
                    else
                        count_geo / number_tweets::double precision end               as rate_geo_tweets,
                case
                    when number_tweets = 0 then -1
                    else
                        count_post / number_tweets::double precision end              as rate_post_tweets,
                case
                    when number_tweets = 0 then -1
                    else
                        count_retweeted / number_tweets::double precision end         as rate_retweeted_tweets,
                case
                    when number_tweets = 0 then -1
                    else
                        count_replied_to / number_tweets::double precision end        as rate_replied_to_tweets,
                case
                    when number_tweets = 0 then -1
                    else
                        count_dominant_language / number_tweets::double precision end as rate_dominant_language_tweets
         from (
                  select t0.user_id_anon,
                         mode() within group (order by month_activity)                  as mode_month_activity,
                         mode() within group (order by day_activity)                    as mode_day_activity,
                         mode() within group (order by day_of_week_activity)            as mode_day_of_week_activity,
                         mode() within group (order by hour_activity)                   as mode_hour_activity,
                         mode() within group (order by type_corrected)                  as mode_type_corrected,
                         mode() within group (order by language)                        as mode_language,
                         count(distinct language)                                       as number_languages,
                         sum(case when possibly_sensitive then 1 else 0 end)            as count_sensitive,
                         sum(case when media_types <> '{}' then 1 else 0 end)           as count_media,
                         sum(case when geo_location then 1 else 0 end)                  as count_geo,
                         sum(case when type_corrected = 'post' then 1 else 0 end)       as count_post,
                         sum(case when type_corrected = 'retweeted' then 1 else 0 end)  as count_retweeted,
                         sum(case when type_corrected = 'replied_to' then 1 else 0 end) as count_replied_to,
                         max(freq_language)                                             as count_dominant_language,
                         count(0),
                         count(distinct tweet_id_anon)                                  as number_tweets,
                         max(less_used_language)                                        as less_used_language,
                         min(less_used_language),
                         max(minutes_since_last_created_at)                             as max_actv_freq_minutes,
                         min(minutes_since_last_created_at)                             as min_actv_freq_minutes,
                         avg(minutes_since_last_created_at)                             as avg_actv_freq_minutes,
                         percentile_disc(0.5)
                         within group (order by minutes_since_last_created_at)          as median_actv_freq_minutes


                  from (
                           select user_id_anon,
                                  tweet_id_anon,
                                  language,
                                  created_at,
                                  extract(month from created_at::timestamp)               as month_activity,
                                  extract(day from created_at::timestamp)                 as day_activity,
                                  extract(isodow from created_at::timestamp)              as day_of_week_activity,
                                  extract(hour from created_at::timestamp)                as hour_activity,
                                  array_length(user_mentions_anon::varchar[], 1)          as number_mentions,
                                  previous_created_at,
                                  extract(epoch from (created_at::timestamp - previous_created_at::timestamp)) /
                                  60                                                      as minutes_since_last_created_at,
                                  possibly_sensitive,
                                  media_types,
                                  geo_location,
                                  case
                                      when user_id_anon = in_reply_to_user_id_anon then 'thread'
                                      when type = 'post' and in_reply_to_user_id_anon is not null then 'replied_to'
                                      when type = 'post' and text like 'RT @%' then 'retweeted'
                                      else type end                                       as type_corrected,
                                  freq_language                                           as freq_language,
                                  first_value(language)
                                  over (partition by user_id_anon order by freq_language) as less_used_language
                           from (
                                    select t0.*,
                                           count(0) over (partition by user_id_anon, language)       as freq_language,
                                           lag(created_at, -1)
                                           over (partition by user_id_anon order by created_at desc) as previous_created_at
                                    from redacted_tables.tweets t0
                                ) t0
                       ) t0
                  group by 1
              ) t0
     )


select -- user features
       t0.user_id_anon,
       t0.year_joined::int                              as uuu_year_joined,
       t0.month_joined::int                             as uuu_month_joined,
       t0.day_of_month_joined::int                      as uuu_day_of_month_joined,
       t0.day_of_week_joined::int                       as uuu_day_of_week_joined,
       t0.joined_weekend::int                           as uuu_joined_weekend,
       t0.hour_joined::int                              as uuu_hour_joined,
       t0.username_length                               as uuu_username_length,
       t0.fraction_numbers_username                     as uuu_fraction_numbers_username,
       t0.fraction_emojis_username                      as uuu_fraction_emojis_username,
       t0.fraction_capital_letters_username             as uuu_fraction_capital_letters_username,
       t0.fraction_special_char_username                as uuu_fraction_special_char_username,
       t0.username_words                                as uuu_username_words,
       t0.avg_word_length_username                      as uuu_avg_word_length_username,
       t0.name_length                                   as uuu_name_length,
       t0.fraction_numbers_name                         as uuu_fraction_numbers_name,
       t0.fraction_emojis_name                          as uuu_fraction_emojis_name,
       t0.fraction_capital_letters_name                 as uuu_fraction_capital_letters_name,
       t0.fraction_special_char_name                    as uuu_fraction_special_char_name,
       t0.name_words                                    as uuu_name_words,
       t0.avg_word_length_name                          as uuu_avg_word_length_name,
       t0.description_tags                              as uuu_description_tags,
       t0.description_mentions                          as uuu_description_mentions,
       t0.description_urls                              as uuu_description_urls,
       t0.has_location                                  as uuu_has_location,
       t0.has_pinned_tweet                              as uuu_has_pinned_tweet,
       t0.has_default_profile_picture                   as uuu_has_default_profile_picture,
       t0.following_count                               as uuu_following_count,
       t0.following_followers_ratio                     as uuu_following_followers_ratio,
       t0.tweet_count                                   as uuu_tweet_count,
       t0.listed_tweet_ratio                            as uuu_listed_tweet_ratio,
       t0.tweet_followers_ratio                         as uuu_tweet_followers_ratio,
       t0.number_countries_withheld                     as uuu_number_countries_withheld,
       t0.is_protected                                  as uuu_is_protected,
       t0.is_verified                                   as uuu_is_verified,
       -- activity features
       t1.total_number_mentions                         as aaa_total_number_mentions,
       -- check
       t1.number_tweets_sample                          as aaa_number_tweets_sample,
       t1.number_tweets_with_mention                    as aaa_number_tweets_with_mention,
       t1.unique_number_mentions                        as aaa_unique_number_mentions,
       t1.avg_number_mentions_tweet                     as aaa_avg_number_mentions_tweet,
       t1.avg_number_mentions_tweet_with_mention        as aaa_avg_number_mentions_tweet_with_mention,
       t1.avg_number_unique_mentions_tweet              as aaa_avg_number_unique_mentions_tweet,
       t1.avg_number_unique_mentions_tweet_with_mention as aaa_avg_number_unique_mentions_tweet_with_mention,
       t1.rate_tweets_with_mention                      as aaa_rate_tweets_with_mention,
       t1.rate_unique_total_mentions                    as aaa_rate_unique_total_mentions,
       t1.max_number_mentions_tweet                     as aaa_max_number_mentions_tweet,
       t2.user_id_anon                                  as aaa_user_id_anon,
       t2.mode_month_activity                           as aaa_mode_month_activity,
       t2.mode_day_activity                             as aaa_mode_day_activity,
       t2.mode_day_of_week_activity                     as aaa_mode_day_of_week_activity,
       t2.mode_hour_activity                            as aaa_mode_hour_activity,
       t2.mode_type_corrected                           as aaa_mode_type_corrected,
       t2.mode_language                                 as aaa_mode_language,
       t2.number_languages                              as aaa_number_languages,
       -- check
       t2.core_number_tweets                            as aaa_core_number_tweets,
       t2.less_used_language                            as aaa_less_used_language,
       t2.max_actv_freq_minutes                         as aaa_max_actv_freq_minutes,
       t2.min_actv_freq_minutes                         as aaa_min_actv_freq_minutes,
       t2.avg_actv_freq_minutes                         as aaa_avg_actv_freq_minutes,
       t2.median_actv_freq_minutes                      as aaa_median_actv_freq_minutes,
       t2.rate_sensitive_tweets                         as aaa_rate_sensitive_tweets,
       t2.rate_media_tweets                             as aaa_rate_media_tweets,
       t2.rate_geo_tweets                               as aaa_rate_geo_tweets,
       t2.rate_post_tweets                              as aaa_rate_post_tweets,
       t2.rate_retweeted_tweets                         as aaa_rate_retweeted_tweets,
       t2.rate_replied_to_tweets                        as aaa_rate_replied_to_tweets,
       t2.rate_dominant_language_tweets                 as aaa_rate_dominant_language_tweets,
       case
           when t3.label = 'bot' then 1
           when t3.label = 'human' then 0
           else -1 end                                  as target
from user_features t0
         left join mentions_features t1
                   on t0.user_id_anon = t1.user_id_anon
         left join core_activity_features t2
                   on t0.user_id_anon = t2.user_id_anon
         left join redacted_tables.labels t3
                   on t0.user_id_anon = t3.user_id_anon;