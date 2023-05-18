import os

import pandas as pd
import tensorflow as tf

import credentials_refactor
import global_settings as gs
from tweetcore.tasks.classification_task.nlp import nlp_classification_tweets as nlp_class
from tweetcore.tasks.postgres_target import download_data, upload_data

if __name__ == '__main__':
    conf = credentials_refactor.return_credentials()
    exclude = gs.exclude_path + '/user_ids_predict.csv'
    checkpoint = gs.model_path + '/optimizer_best_thus_far'
    if os.path.exists(exclude):
        exclude_df = pd.read_csv(exclude)
        exclude_df.loc[:, 'dummy'] = [0] * exclude_df.shape[0]
        upload_data.write_postgre_table(configuration=conf,
                                        data=exclude_df,
                                        table_name='exclude_user_predict',
                                        schema='redacted_tables',
                                        if_exists_then_wat='replace')
        data = download_data.pandas_df_from_postgre_query(configuration=conf,
                                                          query='''
                                                                select t0.*
                                                                    from redacted_tables.tweets_text_classification_post_train t0
                                                                        left join redacted_tables.exclude_user_predict t1
                                                                        on t0.user_id_anon = t1.user_ids_exclude
                                                                where t1.user_ids_exclude is null
                                                                ''')
        print(f'--- excluded {exclude_df.shape[0]} observations already used ---')
    else:
        data = download_data.pandas_df_from_postgre_query(configuration=conf,
                                                          query='''
                                                                select *
                                                                    from redacted_tables.tweets_text_classification_post_train
                                                                    limit 100000 

                                                                ''')
        print('--- starting with all data for the first time ---')

    '''
    model = tf.keras.models.Sequential([
        tf.keras.layers.Input(shape=(gs.max_char_tweet, gs.emb_dim,)),
        tf.keras.layers.Flatten(),
        tf.keras.layers.Dense(256, activation='relu'),
        tf.keras.layers.Dense(128, activation='relu'),
        tf.keras.layers.Dense(64, activation='relu'),
        tf.keras.layers.Dense(32, activation='relu'),
        tf.keras.layers.Dense(1)])
    model.summary()
    model.compile(optimizer='adam',
                  loss=tf.keras.losses.BinaryCrossentropy(from_logits=True),
                  metrics=[tf.keras.metrics.Accuracy(),
                           tf.keras.metrics.Precision(),
                           tf.keras.metrics.Recall(),
                           tf.keras.metrics.AUC(curve='roc', from_logits=True)])
    model.load_weights(checkpoint)
    '''
    model = tf.keras.models.load_model(checkpoint)

    nlp_class.predict(configuration=conf,
                      data=data.head(50000),
                      model=model,
                      exclude_user_ids_path=exclude,
                      strategy='average',
                      batch_size=gs.max_batch_size)
