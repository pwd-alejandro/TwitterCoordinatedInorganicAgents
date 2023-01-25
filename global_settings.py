QUERY_EXISTS = '''  

        SELECT EXISTS (
                   SELECT * 
                   FROM information_schema.tables
                   WHERE  table_schema = '{schema}'
                   AND    table_name   = '{table_name}'
               ) AS EXISTS

'''

QUERY_CHECK = '''  

        SELECT {column}
        FROM {schema}.{table_name}
        WHERE {column} is not null
        ORDER BY {column} DESC
        LIMIT 1

'''

dummy_tweet = '''

    This is a dummy text to max out the number of tokens. This is a dummy text to max out the number of tokens. This 
    is a dummy text to max out the number of tokens. This is a dummy text to max out the number of tokens. This is a 
    dummy text to max out the number of tokens .This is a dummy text to max out the number of tokens. This is a dummy 
    text to max out the number of tokens. This is a dummy text to max out the number of tokens. This is a dummy text 
    to max out the number of tokens. This is a dummy text to max out the number of tokens. This is a dummy text to 
    max out the number of tokens. This is a dummy text to max out the number of tokens. This is a dummy text to max 
    out the number of tokens. This is a dummy text to max out the number of tokens. This is a dummy text to max out 
    the number of tokens .This is a dummy text to max out the number of tokens. This is a dummy text to max out the 
    number of tokens. This is a dummy text to max out the number of tokens. This is a dummy text to max out the 
    number of tokens. This is a dummy text to max out the number of tokens 

'''

max_char_tweet = 140
