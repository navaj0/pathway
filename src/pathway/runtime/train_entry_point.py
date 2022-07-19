import inspect
import os
import pickle
import tarfile
from os import listdir
from os.path import isfile, join, isdir

from .dataset import DataSet, is_data_set

from typing import Callable


def train_script(func: Callable):

    model_dir: str = os.environ['SM_MODEL_DIR']

    signature = inspect.signature(func)
    parameters = signature.parameters

    call_args = {}

    # deserialize the data
    for key in parameters.keys():
        if is_data_set(parameters.get(key).annotation):
            channel = os.environ[f"SM_CHANNEL_{key.upper()}"]
            print(channel)
            if isfile(channel):
                data = DataSet(path=channel)
                data.load()
                call_args[key] = data
            elif isdir(channel):
                data_files = [f for f in listdir(channel) if isfile(join(channel, f))]
                print(data_files)
                if len(data_files) != 1:
                    raise ValueError
                data = DataSet(path=join(channel, data_files[0]))
                data.load()
                call_args[key] = data
        else:
            raise ValueError

    # invoke the train function
    model = func(**call_args)

    # serialize the model
    with open("model.pickle", "wb") as file:
        pickle.dump(model, file)
    with tarfile.open(f"{model_dir}/model.tar.gz", "w:gz") as tar_handle:
        tar_handle.add(f"model.pickle")
