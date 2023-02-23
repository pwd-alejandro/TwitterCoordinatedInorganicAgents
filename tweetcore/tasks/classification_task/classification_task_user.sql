with user_features as (
    select user_id_anon,
           -- datetime joined
           extract('year' from joined_twitter::timestamp)                as year_joined,
           extract('month' from joined_twitter::timestamp)               as month_joined,
           extract('day' from joined_twitter::timestamp)                 as day_of_month_joined,
           extract(isodow from joined_twitter::timestamp)                as day_of_week_joined,
           case
               when extract(isodow from joined_twitter::timestamp) in (6, 7) then 1
               else 0 end                                                as joined_weekend,
           extract('hour' from joined_twitter::timestamp)                as hour_joined,
           -- username
           username_length                                               as username_length,
           username_numbers / username_length::double precision          as fraction_numbers_username,
           username_emojis / username_length::double precision           as fraction_emojis_username,
           username_capital_letters / username_length::double precision  as fraction_capital_letters_username,
           username_special_char / username_length::double precision     as fraction_special_char_username,
           username_words                                                as username_words,
           username_length / username_words::double precision            as avg_word_length_username,
           -- name
           name_length                                                   as name_length,
           name_numbers / name_length::double precision                  as fraction_numbers_name,
           name_emojis / name_length::double precision                   as fraction_emojis_name,
           name_capital_letters / name_length::double precision          as fraction_capital_letters_name,
           name_special_char / name_length::double precision             as fraction_special_char_name,
           name_words                                                    as name_words,
           name_length / name_words::double precision                    as avg_word_length_name,
           -- description
           coalesce(array_length(description_cashtags::varchar[], 1), 0) +
           coalesce(array_length(description_hashtags::varchar[], 1), 0) as description_tags,
           coalesce(array_length(description_mentions::varchar[], 1), 0) as description_mentions,
           coalesce(array_length(description_urls::varchar[], 1), 0)     as description_urls,
           -- other
           case when location is not null then 1 else 0 end              as has_location,
           has_pinned_tweet_id::int                                      as has_pinned_tweet,
           default_profile_picture::int                                  as has_default_profile_picture,
           following_count                                               as following_count,
           following_count / followers_count::double precision           as following_followers_ratio,
           tweet_count                                                   as tweet_count,
           listed_count / tweet_count::double precision                  as listed_tweet_ratio,
           tweet_count / followers_count::double precision               as tweet_followers_ratio,
           coalesce(array_length(withheld::varchar[], 1), 0)             as number_countries_withheld,
           protected::int                                                as is_protected,
           -- what does 'verified' mean now?
           verified::int                                                 as is_verified


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
         select t0.user_id_anon,
                mode() within group (order by month_activity)                              as mode_month_activity,
                mode() within group (order by day_activity)                                as mode_day_activity,
                mode() within group (order by day_of_week_activity)                        as mode_day_of_week_activity,
                mode() within group (order by hour_activity)                               as mode_hour_activity,
                mode() within group (order by type_corrected)                              as mode_type_corrected,
                mode() within group (order by language)                                    as mode_language,
                count(distinct language)                                                   as number_languages,
                sum(case when possibly_sensitive then 1 else 0 end)                        as count_sensitive,
                sum(case when media_types <> '{}' then 1 else 0 end)                       as count_media,
                sum(case when geo_location then 1 else 0 end)                              as count_geo,
                sum(case when type_corrected = 'post' then 1 else 0 end)                   as count_post,
                sum(case when type_corrected = 'retweeted' then 1 else 0 end)              as count_retweeted,
                sum(case when type_corrected = 'replied_to' then 1 else 0 end)             as count_replied_to,
                max(freq_language)                                                         as count_dominant_language,
                count(0),
                count(distinct tweet_id_anon)                                              as number_tweets,
                max(less_used_language)                                                    as less_used_language,
                min(less_used_language),
                max(minutes_since_last_created_at)                                         as max_actv_freq_minutes,
                min(minutes_since_last_created_at)                                         as min_actv_freq_minutes,
                avg(minutes_since_last_created_at)                                         as avg_actv_freq_minutes,
                percentile_disc(0.5) within group (order by minutes_since_last_created_at) as median_actv_freq_minutes


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
                           from redacted_tables.dev_act t0
                       ) t0
              ) t0
         group by 1
     );






