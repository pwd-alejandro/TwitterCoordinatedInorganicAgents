def get_media(entities: dict) -> list:
    if entities is None:
        media_types = []
    elif 'media' in entities:
        media_types = [i['type'] for i in entities['media']]
    else:
        media_types = []
    return media_types


def get_user_mentions(entities: dict) -> list:
    if entities is None:
        ids = []
    elif 'mentions' in entities:
        ids = [str(i['id']) for i in entities['mentions']]
    elif 'user_mentions' in entities:
        ids = [str(i['id_str']) for i in entities['user_mentions']]
    else:
        ids = []
    return ids


def get_type_tweet(referenced_tweets: list = None) -> str:
    if referenced_tweets is not None:
        if len(referenced_tweets) == 2:
            tw_type = 'reply_with_quote'
        elif len(referenced_tweets) == 1:
            tw_type = referenced_tweets[0]["type"]
        else:
            print('More than two referenced tweets')
            print(referenced_tweets)
            raise
    else:
        tw_type = 'post'

    return tw_type
