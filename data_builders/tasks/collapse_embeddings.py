import os
from os import walk

import numpy as np
from IPython.display import clear_output

import global_settings as gs


def clean_temporal(path: str = None,
                   shape: tuple = None) -> str:
    while os.path.exists(path):
        path = path.split('.')
        path = '.'.join([path[0] + '_new', path[1]])
    clean = np.zeros(shape)
    np.save(f'{path}', clean[1:])
    return path


def get_files(dir_path: str = None) -> list:
    filenames = next(walk(dir_path), (None, None, []))[2]
    filenames = [dir_path + '/' + i for i in filenames]
    filenames.sort(key=os.path.getctime)
    filenames = [i.replace(dir_path + '/', '') for i in filenames]
    return filenames


def read_embeddings_simple(path: str = None,
                           files: list = None,
                           number_of_files: int = None,
                           emb_dim: int = 768,
                           collapse_tweet: bool = False,
                           save_on_disk: bool = False) -> (np.memmap, str):
    if files is None:
        filenames = get_files(dir_path=path)

    else:
        number_of_files = len(files)
        filenames = files

    if collapse_tweet:
        save_path = clean_temporal(path=path + '/development_140_final.npy',
                                   shape=(1, emb_dim))
        answer = np.memmap(save_path,
                           mode='w+',
                           dtype=np.float64,
                           shape=(number_of_files * 5000, emb_dim))
        i = 0
        print('--- Starting ---')
        while i < number_of_files:
            response = np.load(path + f'/{filenames[i]}', mmap_mode='r')
            answer[i * response.shape[0]:(i + 1) * response.shape[0], :] = np.mean(response, axis=1)
            i += 1
            clear_output(wait=True)
            print(f'{i}/{number_of_files}')
        print('--- Ended ---')
    else:
        save_path = clean_temporal(path=path + '/development_140_final.npy',
                                   shape=(1, gs.max_char_tweet, emb_dim))
        answer = np.memmap(save_path,
                           mode='w+',
                           dtype=np.float64,
                           shape=(number_of_files * 5000, gs.max_char_tweet, emb_dim))
        i = 0
        print('--- Starting ---')
        while i < number_of_files:
            response = np.load(path + f'/{filenames[i]}', mmap_mode='r')
            answer[i * response.shape[0]:(i + 1) * response.shape[0], :, :] = response
            i += 1
            clear_output(wait=True)
            print(f'{i}/{number_of_files}')
        print('--- Ended ---')
    permanent_path = None
    if save_on_disk:
        permanent_path = save_path.split('.')
        permanent_path = '.'.join([permanent_path[0] + '_permanent', permanent_path[1]])
        np.save(permanent_path, answer)

    return answer, permanent_path


def read_embeddings_advanced(path: str = None,
                             indices: list = None,
                             emb_dim: int = 768):
    sorted_indices = sorted(indices)
    filenames = next(walk(path), (None, None, []))[2]
    filenames = [path + '/' + i for i in filenames]
    filenames.sort(key=os.path.getctime)
    filenames = [i.replace(path + '/', '') for i in filenames]

    sentinel_size = 0
    sentinel_indices = 0
    normalization = [sentinel_size]
    answer = np.memmap(path + '/development_140_final.npy', mode='r+', shape=(len(indices), gs.max_char_tweet, emb_dim))
    response = None
    for i in range(len(sorted_indices)):
        while sorted_indices[i] >= sentinel_size:
            file = filenames[sentinel_indices]
            response = np.load(path + f'/{file}', mmap_mode='r')
            sentinel_indices += 1
            sentinel_size += response.shape[0]
            normalization.append(sentinel_size)

        answer[i, :, :] = response[[i - normalization[sentinel_indices - 1] for i in [sorted_indices[i]]]]

    return answer

# path_140 = f'{gs.data_path}/nlp/embeddings/140'
# test = read_embeddings_simple(path=path_140,
#                              #files=['development_140_6000.npy'],
#                              number_of_files=1,
#                              collapse_tweet=True)
# breakpoint()
# print('Done')
