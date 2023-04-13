import gc
import os

import numpy as np
import pandas as pd
import tensorflow as tf
from transformers import AutoModel
from transformers import AutoTokenizer

import credentials_refactor
import global_settings as gs
from tweetcore.tasks.postgres_target import download_data, upload_data


def get_representation(data: pd.Series = None,
                       model_name: str = 'distilbert-base-uncased',
                       where: str = 'cuda'):
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModel.from_pretrained(model_name).to(where)
    tokens = tokenizer(list(data),
                       truncation=True,
                       max_length=gs.max_char_tweet,
                       return_tensors='pt',
                       padding=True).to(where)
    inputs = tokens.input_ids
    attention = tokens.attention_mask
    embed = model(input_ids=inputs,
                  attention_mask=attention,
                  output_attentions=False,
                  output_hidden_states=False)
    representation = embed.last_hidden_state.to('cpu').detach().numpy()
    return representation


def train_model_on_the_go(model: tf.keras.models = None,
                          x_train: pd.Series = None,
                          y_train: pd.Series = None,
                          save_checkpoint: str = None,
                          **kwargs):
    model_checkpoint_callback = tf.keras.callbacks.ModelCheckpoint(
        filepath=save_checkpoint,
        save_weights_only=True,
        monitor='loss',
        mode='min',
        save_best_only=True)

    callback_loss = tf.keras.callbacks.EarlyStopping(monitor='loss',
                                                     patience=10)

    x_train_representation = get_representation(data=x_train,
                                                model_name='distilbert-base-uncased',
                                                where='cuda')
    model.fit(x_train_representation,
              y_train,
              callbacks=[model_checkpoint_callback, callback_loss],
              **kwargs)
    del x_train_representation
    gc.collect()


def train_text_classifier(model,
                          x_train,
                          y_train,
                          save_checkpoint: str = None,
                          exclude_user_ids_path: str = None,
                          batch_size: int = gs.max_batch_size,
                          **kwargs):
    splits = round(x_train.shape[0] / batch_size) + 1

    if os.path.exists(exclude_user_ids_path):
        exclude_user_ids = pd.read_csv(exclude_user_ids_path)
    else:
        exclude_user_ids = None

    indices_splits = np.array_split(x_train.index, splits)
    for indices_batch in indices_splits:
        x_train_batch = x_train.iloc[indices_batch]
        y_train_batch = y_train.iloc[indices_batch]

        if save_checkpoint is not None:
            model.load_weights(save_checkpoint)

        train_model_on_the_go(model=model,
                              x_train=x_train_batch,
                              y_train=y_train_batch,
                              save_checkpoint=save_checkpoint,
                              **kwargs)

        temp_exclude = pd.DataFrame(data={'user_ids_exclude': indices_batch})
        exclude_user_ids = pd.concat([exclude_user_ids, temp_exclude])
        exclude_user_ids.to_csv(exclude_user_ids_path)


conf = credentials_refactor.return_credentials()
exclude = gs.exclude_path + '\\user_ids.csv'
if os.path.exists(exclude):
    exclude_df = pd.read_csv(exclude)
    upload_data.write_postgre_table(configuration=conf,
                                    data=exclude_df,
                                    table_name='exclude_user_ids_from_training',
                                    schema='redacted_tables',
                                    if_exists_then_wat='replace')
    data = download_data.pandas_df_from_postgre_query(configuration=conf,
                                                      query='''
                                                              select *
                                                              from (select *
                                                                    from redacted_tables.tweets_text_classification_post
                                                                    left join redacted_tables.exclude_user_ids_from_training t1
                                                                on t0.user_id_anon = t1.user_id_anon
                                                                where t1.user_id_anon is null




                                                            ''')

data_dev = download_data.pandas_df_from_postgre_query(configuration=conf,
                                                      query='''
                                              select *
                                              from redacted_tables.tweets_text_classification_post
            --                                  limit 25
                                              ''')

print(data_dev.shape)
'''
representationn = get_representation(data=data_dev.agg_text,
                                     model_name='distilbert-base-uncased',
                                     where='cuda')
print(representationn.shape)
'''
