import ijson
import pandas as pd
import credentials_refactor
from tweetcore.tasks.postgres_target import upload_data
from tweetcore.twitter_utils import tweet_utils


def migrate_tweets(path_to_external_data: str = None,
                   hit_table: str = None,
                   hit_schema: str = None,
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
                    'referenced_tweets': [record['referenced_tweets']],
                    'user_mentions': [tweet_utils.get_user_mentions(record['entities'])],
                    'media_types': [tweet_utils.get_media(record['entities'])],
                    'in_reply_to_user_id': record['in_reply_to_user_id'],
                    'possibly_sensitive': record['possibly_sensitive']
                }
            )

            resp = pd.concat([resp, temp_df])

            i += 1
            if (i > 0) & (i % 10000 == 0):
                upload_data.write_postgre_table(configuration=config,
                                                data=resp,
                                                table_name=hit_table,
                                                schema=hit_schema,
                                                if_exists_then_wat='append')
                resp = None

        breakpoint()
        upload_data.write_postgre_table(configuration=config,
                                        data=resp,
                                        table_name=hit_table,
                                        schema=hit_schema,
                                        if_exists_then_wat='append')


conf = credentials_refactor.return_credentials()

migrate_tweets(path_to_external_data="../../external_data/tweet_0.json",
               hit_table='test',
               hit_schema='tweetcore',
               config=conf)


