import gc

import numpy as np
import pandas as pd
import torch
from transformers import AutoModel
from transformers import AutoTokenizer

import credentials_refactor
import global_settings as gs
from tweetcore.tasks.postgres_target import download_data


def get_representation(data: pd.Series = None,
                       model_name: str = 'distilbert-base-uncased',
                       save_path: str = None,
                       splits: int = 30):
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModel.from_pretrained(model_name)
    emb_dim = model.config.dim
    representation = np.zeros((1, gs.max_char_tweet, emb_dim))
    i = 0
    for data_batch in np.array_split(data.values, splits):
        tokens_batch = tokenizer([gs.dummy_tweet] + list(data_batch),
                                 truncation=True,
                                 max_length=gs.max_char_tweet,
                                 return_tensors='pt',
                                 padding=True)

        batch_input = tokens_batch.input_ids[1:, :]
        batch_attention = tokens_batch.attention_mask[1:, :]
        embed_batch = model(input_ids=batch_input,
                            attention_mask=batch_attention,
                            output_attentions=False,
                            output_hidden_states=False)
        representation_batch = embed_batch.last_hidden_state.detach().numpy()
        representation = np.concatenate((representation, representation_batch), axis=0)
        del embed_batch, representation_batch
        gc.collect()
        i += 1
        print(f'Done {i}/{splits}')
    np.save(f'{save_path}', representation[1:, :, :])
    del representation
    gc.collect()


conf = credentials_refactor.return_credentials()
data_dev = download_data.pandas_df_from_postgre_query(configuration=conf,
                                                      query='''
                                                      select *
                                                      from tweetcore.migration_development_200k
                                                      order by tweet_id
                                                      limit 100
                                                      '''
                                                      )
get_representation(data=data_dev.text,
                   model_name='distilbert-base-uncased',
                   save_path='../../../data/nlp/embeddings/test',
                   splits=1)
