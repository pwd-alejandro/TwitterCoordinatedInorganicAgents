def get_description_entities(entities: dict) -> (list, list, list, list):
    if "description" not in entities.keys():
        return [], [], [], []
    else:
        cashtags = None
        hashtags = None
        mentions = None
        urls = None

        if 'cashtags' in entities["description"].keys():
            cashtags = [i["tag"] for i in entities["description"]['cashtags']]
        if 'hashtags' in entities["description"].keys():
            hashtags = [i["tag"] for i in entities["description"]['hashtags']]
        if 'mentions' in entities["description"].keys():
            mentions = [i["username"] for i in entities["description"]['mentions']]
        if 'urls' in entities["description"].keys():
            urls = [i["display_url"] for i in entities["description"]['urls']]

    return cashtags, hashtags, mentions, urls


def get_withheld_countries(record) -> list:
    if record["withheld"] is None:
        return []
    else:
        return record["withheld"]["country_codes"]
