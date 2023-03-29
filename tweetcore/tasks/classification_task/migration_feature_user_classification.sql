drop table redacted_tables.features_user_classification;

create table redacted_tables.features_user_classification as
select user_id_anon,
       uuu_year_joined::int,
       uuu_month_joined::int,
       uuu_day_of_month_joined::int,
       uuu_day_of_week_joined::int,
       uuu_joined_weekend,
       uuu_hour_joined::int,
       uuu_username_length,
       uuu_fraction_numbers_username,
       uuu_fraction_emojis_username,
       uuu_fraction_capital_letters_username,
       uuu_fraction_special_char_username,
       uuu_username_words,
       uuu_avg_word_length_username,
       uuu_name_length,
       uuu_fraction_numbers_name,
       uuu_fraction_emojis_name,
       uuu_fraction_capital_letters_name,
       uuu_fraction_special_char_name,
       uuu_name_words,
       uuu_avg_word_length_name,
       uuu_description_tags,
       uuu_description_mentions,
       uuu_description_urls,
       uuu_has_location,
       uuu_has_pinned_tweet,
       uuu_has_default_profile_picture,
       uuu_following_count,
       uuu_following_followers_ratio,
       uuu_tweet_count,
       uuu_listed_tweet_ratio,
       uuu_tweet_followers_ratio,
       uuu_number_countries_withheld,
       uuu_is_protected,
       uuu_is_verified,
       aaa_total_number_mentions,
       aaa_number_tweets_sample,
       aaa_number_tweets_with_mention,
       aaa_unique_number_mentions,
       aaa_avg_number_mentions_tweet,
       aaa_avg_number_mentions_tweet_with_mention,
       aaa_avg_number_unique_mentions_tweet,
       aaa_avg_number_unique_mentions_tweet_with_mention,
       aaa_rate_tweets_with_mention,
       aaa_rate_unique_total_mentions,
       aaa_max_number_mentions_tweet,
       aaa_mode_month_activity::float8,
       aaa_mode_day_activity::float8,
       aaa_mode_day_of_week_activity::float8,
       aaa_mode_hour_activity::float8,
       aaa_mode_type_corrected,
       aaa_mode_language,
       aaa_number_languages,
       aaa_less_used_language,
       aaa_max_actv_freq_minutes::float8,
       aaa_min_actv_freq_minutes::float8,
       aaa_avg_actv_freq_minutes::float8,
       aaa_median_actv_freq_minutes::float8,
       aaa_rate_sensitive_tweets,
       aaa_rate_media_tweets,
       aaa_rate_geo_tweets,
       aaa_rate_post_tweets,
       aaa_rate_retweeted_tweets,
       aaa_rate_replied_to_tweets,
       aaa_rate_dominant_language_tweets,
       target
from redacted_tables.features_user_classification_backup;

