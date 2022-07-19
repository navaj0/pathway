from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TypeVar, Generic, Any
from urllib.parse import urlparse

import boto3
import pickle
import pathlib
import io
import time


primitives = (int, float, bool, str)


@dataclass
class Input:
    path: str


@dataclass
class Output:
    path: str


class _PickleDataLoader:

    @staticmethod
    def load_from_s3(s3_uri: str):
        with Timer(name=f"unpickle from s3: {s3_uri}"):
            bucket, object_key = parse_s3_url(s3_uri)
            s3_resource = boto3.resource("s3")
            bytes_io = io.BytesIO()
            s3_resource.Bucket(bucket).download_fileobj(object_key, bytes_io)
            bytes_io.seek(0)
            return pickle.loads(bytes_io.read())

    @staticmethod
    def save_to_s3(data: Any, s3_uri: str):
        with Timer(name=f"pickle to s3: {s3_uri}"):
            bucket, object_key = parse_s3_url(s3_uri)
            s3 = boto3.resource("s3")
            data_file = io.BytesIO(pickle.dumps(data))
            s3.Bucket(bucket).upload_fileobj(data_file, object_key)

    @staticmethod
    def load_from_local(path: str):
        with open(path, "rb") as file:
            data = pickle.load(file)
        return data

    @staticmethod
    def save_to_local(data: Any, path: str):
        if data is None:
            raise ValueError

        with open(path, "wb") as file:
            pickle.dump(data, file)



class Job(ABC):
    def __init__(self, results=None):
        self._results = results

    @abstractmethod
    def wait(self, logs=True):
        """Wait for the Amazon SageMaker job to finish."""

    @abstractmethod
    def stop(self):
        """Stop the job."""

    @abstractmethod
    def is_completed(self):
        """ check if the job is completed """

    @property
    def results(self):
        return self._results


class AbstractDataObject(ABC):

    @abstractmethod
    def get(self) -> Any:
        raise NotImplementedError


@dataclass
class DataObject(AbstractDataObject):
    data: Any

    @classmethod
    def set(cls, data: Any):
        return cls(data=data)

    def get(self):
        return self.data


class _AsyncDataObject(AbstractDataObject):
    def __init__(self, job: Job, path: str):
        self._job = job
        self._path = path
        self._data = None

    def get(self) -> Any:
        if self._data is not None:
            return self._data

        if self._job.is_completed():
            # load
            self._data = _PickleDataLoader.load_from_s3(self._path)
            return self._data

        raise ValueError


def _is_data_object(type_annotation):
    return issubclass(type_annotation, AbstractDataObject)


def is_primitive(type_annotation):
    return type_annotation in (int, bool, float, str)


def parse_s3_url(url):
    """Returns an (s3 bucket, key name/prefix) tuple from a url with an s3 scheme.

    Args:
        url (str):

    Returns:
        tuple: A tuple containing:

            - str: S3 bucket name
            - str: S3 key
    """
    parsed_url = urlparse(url)
    if parsed_url.scheme != "s3":
        raise ValueError("Expecting 's3' scheme, got: {} in {}.".format(parsed_url.scheme, url))
    return parsed_url.netloc, parsed_url.path.lstrip("/")


def s3_path_join(*args):
    """Returns the arguments joined by a slash ("/"), similarly to ``os.path.join()`` (on Unix).

    If the first argument is "s3://", then that is preserved.

    Args:
        *args: The strings to join with a slash.

    Returns:
        str: The joined string.
    """
    if args[0].startswith("s3://"):
        path = str(pathlib.PurePosixPath(*args[1:])).lstrip("/")
        return str(pathlib.PurePosixPath(args[0], path)).replace("s3:/", "s3://")

    return str(pathlib.PurePosixPath(*args)).lstrip("/")


class Timer:
    def __init__(self, name: str):
        self._start = None
        if name is None:
            self._name = "Elapsed time"
        else:
            self._name = name

    def __enter__(self):
        self._start = time.perf_counter()

    def __exit__(self, exc_type, exc_val, exc_tb):
        stop = time.perf_counter()
        print(f"{self._name}: {stop - self._start} s")