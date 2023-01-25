import gc

import numpy as np
import pandas as pd
from transformers import AutoModel
from transformers import AutoTokenizer

import credentials_refactor
import global_settings as gs
from tweetcore.tasks.postgres_target import download_data


def get_representation(data: pd.Series = None,
                       model_name: str = 'distilbert-base-uncased',
                       save_path: str = None,
                       resume_checkpoint: int = None,
                       splits: int = 30):
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModel.from_pretrained(model_name).to('cuda')
    emb_dim = model.config.dim
    representation = np.zeros((1, gs.max_char_tweet, emb_dim))
    data_splits = np.array_split(data.values, splits)
    i = 0
    for data_batch in data_splits[resume_checkpoint+1:]:
        tokens_batch = tokenizer([gs.dummy_tweet] + list(data_batch),
                                 truncation=True,
                                 max_length=gs.max_char_tweet,
                                 return_tensors='pt',
                                 padding=True).to('cuda')
        batch_input = tokens_batch.input_ids[1:, :]
        batch_attention = tokens_batch.attention_mask[1:, :]
        embed_batch = model(input_ids=batch_input,
                            attention_mask=batch_attention,
                            output_attentions=False,
                            output_hidden_states=False)
        representation_batch = embed_batch.last_hidden_state.to('cpu').detach().numpy()
        representation = np.concatenate((representation, representation_batch), axis=0)
        del embed_batch, representation_batch, tokens_batch, batch_input, batch_attention
        gc.collect()
        i += 1
        if (i > 0) & (i % 50 == 0):
            np.save(f'{save_path}_{resume_checkpoint + i}', representation[1:, :, :])
            print(f'Saved at {resume_checkpoint + i}')
            del representation
            gc.collect()
            representation = np.zeros((1, gs.max_char_tweet, emb_dim))
        print(f'Done {resume_checkpoint + i}/{splits}')

    np.save(f'{save_path}_final', representation[1:, :, :])
    del representation
    gc.collect()


conf = credentials_refactor.return_credentials()
data_dev = download_data.pandas_df_from_postgre_query(configuration=conf,
                                                      query='''
                                                      select *
                                                      from tweetcore.migration_development
                                                      order by index
                                                      --limit 100
                                                      '''
                                                      )
# breakpoint()
get_representation(data=data_dev.text,
                   model_name='distilbert-base-uncased',
                   save_path=f'../../../data/nlp/embeddings/{str(gs.max_char_tweet)}_2/development_{str(gs.max_char_tweet)}',
                   resume_checkpoint=1850,
                   splits=6000)
