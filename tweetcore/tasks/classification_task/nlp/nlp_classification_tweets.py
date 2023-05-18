import gc
import os

import absl.logging
import numpy as np
import pandas as pd
import tensorflow as tf
from scipy.special import expit
from transformers import AutoModel
from transformers import AutoTokenizer

import global_settings as gs
from tweetcore.tasks.postgres_target import upload_data


def new_model(strategy: str = 'flatten'):
    if strategy == 'flatten':
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
    elif strategy == 'average':
        model = tf.keras.models.Sequential([
            tf.keras.layers.Input(shape=(gs.emb_dim,)),
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
    else:
        print('--- select valid strategy: flatten or average ---')
        raise

    return model


def get_representation(data: pd.Series = None,
                       model_name: str = 'distilbert-base-uncased',
                       where: str = 'cuda',
                       strategy: str = 'flatten'):
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModel.from_pretrained(model_name).to(where)
    tokens = tokenizer(list(data),
                       truncation=True,
                       max_length=gs.max_char_tweet,
                       return_tensors='pt',
                       padding='max_length').to(where)
    inputs = tokens.input_ids
    attention = tokens.attention_mask
    embed = model(input_ids=inputs,
                  attention_mask=attention,
                  output_attentions=False,
                  output_hidden_states=False)
    if strategy == 'flatten':
        representation = embed.last_hidden_state.to('cpu').detach().numpy()
    elif strategy == 'average':
        representation_temp = embed.last_hidden_state.to('cpu').detach().numpy()
        representation = np.mean(representation_temp, axis=1)
    else:
        print('--- select valid strategy: flatten or average ---')
        raise

    return representation


def train_model_on_the_go(model: tf.keras.models = None,
                          x_train: pd.Series = None,
                          y_train: pd.Series = None,
                          save_checkpoint: str = None,
                          strategy: str = 'flatten',
                          **kwargs):
    absl.logging.set_verbosity(absl.logging.ERROR)
    model_checkpoint_callback = tf.keras.callbacks.ModelCheckpoint(
        filepath=save_checkpoint,
        save_weights_only=False,
        monitor='loss',
        mode='min',
        save_best_only=False)

    callback_loss = tf.keras.callbacks.EarlyStopping(monitor='loss',
                                                     patience=10)

    x_train_representation = get_representation(data=x_train,
                                                model_name='distilbert-base-uncased',
                                                where='cuda',
                                                strategy=strategy)
    model.fit(x_train_representation,
              y_train,
              callbacks=[model_checkpoint_callback, callback_loss],
              **kwargs)
    del x_train_representation
    gc.collect()


def train_text_classifier(data: pd.DataFrame = None,
                          save_checkpoint: str = None,
                          exclude_user_ids_path: str = None,
                          batch_size: int = gs.max_batch_size,
                          strategy: str = 'flatten',
                          **kwargs):
    splits = round(data.shape[0] / batch_size) + 1

    if os.path.exists(exclude_user_ids_path):
        exclude_user_ids = pd.read_csv(exclude_user_ids_path)
    else:
        exclude_user_ids = None
    if not os.path.exists('/'.join(exclude_user_ids_path.split('/')[:-1])):
        os.mkdir('/'.join(exclude_user_ids_path.split('/')[:-1]))

    indices_splits = np.array_split(data.index, splits)
    i = 0
    print(f'--- training in {splits} rounds ---')
    for indices_batch in indices_splits:
        data_batch = data.iloc[indices_batch]

        if os.path.exists('/'.join(save_checkpoint.split('/')[:-1])):
            model = tf.keras.models.load_model(save_checkpoint)
        else:
            model = new_model(strategy=strategy)
            print('new model')

        train_model_on_the_go(model=model,
                              x_train=data_batch.agg_text,
                              y_train=data_batch.target,
                              save_checkpoint=save_checkpoint,
                              strategy=strategy,
                              **kwargs)
        temp_exclude = pd.DataFrame(data={'user_ids_exclude': data_batch.user_id_anon})
        exclude_user_ids = pd.concat([exclude_user_ids, temp_exclude])
        exclude_user_ids.to_csv(exclude_user_ids_path,
                                index=False)

        i += 1
        print(f'--- {i}/{splits} done ---')

        # try:
        #    inputimeout(prompt='--- you have 2s to stop the code ---', timeout=2)
        # except TimeoutOccurred:
        #    print('--- code will keep on running ---')
        # clear_output(wait=False)


def predict(configuration: dict = None,
            data: pd.DataFrame = None,
            model: tf.keras.Model = None,
            exclude_user_ids_path: str = None,
            strategy: str = None,
            batch_size: int = gs.max_batch_size, ):
    splits = round(data.shape[0] / batch_size) + 1
    indices_splits = np.array_split(data.index, splits)

    if os.path.exists(exclude_user_ids_path):
        exclude_user_ids = pd.read_csv(exclude_user_ids_path)
    else:
        exclude_user_ids = None

    i = 0
    print(f'--- predicting in {splits} rounds ---')
    for indices_batch in indices_splits:
        data_batch = data.iloc[indices_batch]
        representation = get_representation(data=data_batch.agg_text,
                                            strategy=strategy)
        y_prediction = np.array([expit(i[0]) for i in model.predict(representation)])
        export = pd.DataFrame(data={'user_id_anon': data_batch.user_id_anon,
                                    'y_prediction': y_prediction,
                                    'y_true': data_batch.target})
        upload_data.write_postgre_table(configuration=configuration,
                                        data=export,
                                        table_name='tweets_text_prediction',
                                        schema='redacted_tables',
                                        if_exists_then_wat='append')

        temp_exclude = pd.DataFrame(data={'user_ids_exclude': data_batch.user_id_anon})
        exclude_user_ids = pd.concat([exclude_user_ids, temp_exclude])
        if not os.path.exists('/'.join(exclude_user_ids_path.split('/')[:-1])):
            os.mkdir('/'.join(exclude_user_ids_path.split('/')[:-1]))
        exclude_user_ids.to_csv(exclude_user_ids_path,
                                index=False)

        i += 1
        print(f'--- {i}/{splits} done ---')

        del representation
        gc.collect()
