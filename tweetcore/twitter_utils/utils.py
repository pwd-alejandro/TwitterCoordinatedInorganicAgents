import emoji


def get_text_attributes(text: str = None,
                        word_delimiter: str = None) -> (int, int, int, int, int):
    length = len(text)
    words = len(text.split(word_delimiter))
    numbers = len([i for i in text if i.isnumeric()])
    special_char = len([i for i in text.replace(' ', '') if not i.isnumeric() and not i.isalpha()])
    emojis = len([i for i in text if emoji.is_emoji(i)])
    capital_letters = len([i for i in text if i.isupper()])

    return length, words, numbers, special_char, emojis, capital_letters
