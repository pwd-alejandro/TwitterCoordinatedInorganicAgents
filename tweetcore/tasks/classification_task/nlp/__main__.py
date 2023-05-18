import os

import pandas as pd

import credentials_refactor
import global_settings as gs
from tweetcore.tasks.classification_task.nlp import nlp_classification_tweets as nlp_class
from tweetcore.tasks.postgres_target import download_data, upload_data

# to start fresh, make sure to:
# 1. Delete the user_ids.csv that will be excluded. Find them at gs.exclude_path
# 2. Drop the table redacted_tables.exclude_user_training
# 3. Delete the folder model in gs.model_path

if __name__ == '__main__':
    conf = credentials_refactor.return_credentials()
    exclude = gs.exclude_path + '/user_ids.csv'
    checkpoint = gs.model_path + '/optimizer_best_thus_far'
    if os.path.exists(exclude):
        exclude_df = pd.read_csv(exclude)
        exclude_df.loc[:, 'dummy'] = [0] * exclude_df.shape[0]
        upload_data.write_postgre_table(configuration=conf,
                                        data=exclude_df,
                                        table_name='exclude_user_training',
                                        schema='redacted_tables',
                                        if_exists_then_wat='replace')
        data = download_data.pandas_df_from_postgre_query(configuration=conf,
                                                          query='''
                                                                select t0.*
                                                                    from redacted_tables.tweets_text_classification_post_train t0
                                                                        left join redacted_tables.exclude_user_training t1
                                                                        on t0.user_id_anon = t1.user_ids_exclude
                                                                where t1.user_ids_exclude is null
                                                                ''')
        print(f'--- excluded {exclude_df.shape[0]} observations already used ---')
    else:
        data = download_data.pandas_df_from_postgre_query(configuration=conf,
                                                          query='''
                                                                select *
                                                                    from redacted_tables.tweets_text_classification_post_train
                                                                ''')
        print('--- starting with all data for the first time ---')

    nlp_class.train_text_classifier(data=data.head(37000),
                                    save_checkpoint=checkpoint,
                                    exclude_user_ids_path=exclude,
                                    batch_size=gs.max_batch_size,
                                    strategy='average',
                                    **{'epochs': 1, 'verbose': 1})

