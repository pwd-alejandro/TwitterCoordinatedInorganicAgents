import ijson
import pandas as pd

import credentials_refactor
from tweetcore.tasks.postgres_target import upload_data
from tweetcore.twitter_utils import tweet_utils, user_utils, utils


def migrate_tweets(path_to_external_data: str = None,
                   hit_table: str = None,
                   hit_schema: str = None,
                   resume: int = None,
                   config: dict = None) -> None:
    with open(path_to_external_data, "rb") as f:
        i = 0
        resp = None
        for record in ijson.items(f, "item"):

            temp_df = pd.DataFrame(
                data={
                    'id': record['id'],
                    'author_id': record['author_id'],
                    'created_at': record['created_at'],
                    'type': tweet_utils.get_type_tweet(referenced_tweets=record['referenced_tweets']),
                    'text': record["text"],
                    'language': record["lang"],
                    'geo_location': record['geo'] is not None,
                    'referenced_tweets_types': [
                        tweet_utils.get_referenced_tweets_types(record["referenced_tweets"])],
                    'referenced_tweets_ids': [tweet_utils.get_referenced_tweets_ids(record["referenced_tweets"])],
                    'user_mentions': [tweet_utils.get_user_mentions(record['entities'])],
                    'media_types': [tweet_utils.get_media(record['entities'])],
                    'in_reply_to_user_id': record['in_reply_to_user_id'],
                    'possibly_sensitive': record['possibly_sensitive']
                }
            )

            resp = pd.concat([resp, temp_df])

            i += 1
            if (i > 0) & (i % 10000 == 0):
                if i > resume:
                    print(f'Resuming at {i}')
                    upload_data.write_postgre_table(configuration=config,
                                                    data=resp,
                                                    table_name=hit_table,
                                                    schema=hit_schema,
                                                    if_exists_then_wat='append')
                resp = None

        upload_data.write_postgre_table(configuration=config,
                                        data=resp,
                                        table_name=hit_table,
                                        schema=hit_schema,
                                        if_exists_then_wat='append')


def migrate_users(path_to_external_data: str = None,
                  hit_table: str = None,
                  hit_schema: str = None,
                  resume: int = None,
                  config: dict = None) -> None:
    print(f'--- Resuming at {resume} ---')
    with open(path_to_external_data, "rb") as f:
        i = 0
        resp = None
        for record in ijson.items(f, "item"):
            i += 1
            if i % 100000 == 0:
                print(i)
            if i > resume:
                if record["entities"] is not None:
                    cashtags, hashtags, mentions, urls = user_utils.get_description_entities(
                        entities=record['entities'])
                else:
                    cashtags, hashtags, mentions, urls = None, None, None, None

                name_length, name_words, name_numbers, \
                name_special_char, name_emojis, name_capital_letters = utils.get_text_attributes(
                    text=record["name"],
                    word_delimiter=' ')
                username_length, username_words, username_numbers, \
                username_special_char, username_emojis, username_capital_letters = utils.get_text_attributes(
                    text=record["username"],
                    word_delimiter='_')
                temp_df = pd.DataFrame(
                    data={
                        'user_id': record['id'],
                        'joined_twitter': record['created_at'],
                        'username': record["username"],
                        'username_length': username_length,
                        'username_words': username_words,
                        'username_numbers': username_numbers,
                        'username_special_char': username_special_char,
                        'username_emojis': username_emojis,
                        'username_capital_letters': username_capital_letters,
                        'name': record["name"],
                        'name_length': name_length,
                        'name_words': name_words,
                        'name_numbers': name_numbers,
                        'name_special_char': name_special_char,
                        'name_emojis': name_emojis,
                        'name_capital_letters': name_capital_letters,
                        'description': record["description"].replace("\x00", "\uFFFD"),
                        'description_cashtags': [cashtags],
                        'description_hashtags': [hashtags],
                        'description_mentions': [mentions],
                        'description_urls': [urls],
                        'location': record["location"],
                        'pinned_tweet_id': record["pinned_tweet_id"],
                        'default_profile_picture': 'default_profile_normal' in record["profile_image_url"],
                        'followers_count': record["public_metrics"]["followers_count"],
                        'following_count': record["public_metrics"]["following_count"],
                        'tweet_count': record["public_metrics"]["tweet_count"],
                        'listed_count': record["public_metrics"]["listed_count"],
                        'url': record["url"],
                        'withheld': [user_utils.get_withheld_countries(record)],
                        'protected': record["protected"],
                        'verified': record["verified"],
                    }
                )
                resp = pd.concat([resp, temp_df])

                if (i > 0) & (i % 10000 == 0):
                    upload_data.write_postgre_table(configuration=config,
                                                    data=resp,
                                                    table_name=hit_table,
                                                    schema=hit_schema,
                                                    if_exists_then_wat='append')
                    resp = None
        upload_data.write_postgre_table(configuration=config,
                                        data=resp,
                                        table_name=hit_table,
                                        schema=hit_schema,
                                        if_exists_then_wat='append')


conf = credentials_refactor.return_credentials()
migrate_users(path_to_external_data="../../external_data/user.json",
              hit_table='users',
              hit_schema='tweetcore',
              resume=0,
              config=conf)
# breakpoint()
# migrate_tweets(path_to_external_data="../../external_data/tweet_2.json",
#               hit_table='tweet_2',
#               hit_schema='tweetcore',
#               resume=0,
#               config=conf)
