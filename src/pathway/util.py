import contextlib
import cloudpickle
import shutil
import subprocess
import shlex
import tempfile
import time

from typing import Callable


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


def pickle_func(func: Callable, session, bucket: str, s3_key_prefix: str):
    """
    Package source files and upload a compress tar file to S3.
    """

    pickled = cloudpickle.dumps(func)

    s3_resource = session.resource("s3", region_name=session.region_name)
    s3_resource.Object(bucket, f'{s3_key_prefix}/func.pkl').put(Body=pickled)

    return f's3://{bucket}/{s3_key_prefix}/func.pkl'


def pickle_object(func: Callable, session, bucket: str, s3_key_prefix: str):
    """
    Package source files and upload a compress tar file to S3.
    """

    pickled = cloudpickle.dumps(func)

    s3_resource = session.resource("s3", region_name=session.region_name)
    s3_resource.Object(bucket, f'{s3_key_prefix}/func.pkl').put(Body=pickled)

    return f's3://{bucket}/{s3_key_prefix}/func.pkl'


def unpickle_object(func: Callable, session, bucket: str, s3_key_prefix: str):
    """
    Package source files and upload a compress tar file to S3.
    """

    pickled = cloudpickle.dumps(func)

    s3_resource = session.resource("s3", region_name=session.region_name)
    s3_resource.Object(bucket, f'{s3_key_prefix}/func.pkl').put(Body=pickled)

    return f's3://{bucket}/{s3_key_prefix}/func.pkl'


def sagemaker_timestamp():
    """Return a timestamp with millisecond precision."""
    moment = time.time()
    moment_ms = repr(moment).split(".")[1][:3]
    return time.strftime("%Y-%m-%d-%H-%M-%S-{}".format(moment_ms), time.gmtime(moment))


@contextlib.contextmanager
def tmpdir(suffix="", prefix="tmp"):
    """Create a temporary directory with a context manager.

    The file is deleted when the context exits.
    The prefix, suffix, and dir arguments are the same as for mkstemp().

    Args:
        suffix (str): If suffix is specified, the file name will end with that
            suffix, otherwise there will be no suffix.
        prefix (str): If prefix is specified, the file name will begin with that
            prefix; otherwise, a default prefix is used.

    Returns:
        str: path to the directory
    """
    tmp = tempfile.mkdtemp(suffix=suffix, prefix=prefix, dir=None)
    try:
        yield tmp
    finally:
        shutil.rmtree(tmp)


def check_output(cmd, *popenargs, **kwargs):
    """Makes a call to `subprocess.check_output` for the given command and args.

    Args:
        cmd:
        *popenargs:
        **kwargs:
    """
    if isinstance(cmd, str):
        cmd = shlex.split(cmd)

    success = True
    try:
        output = subprocess.check_output(cmd, *popenargs, **kwargs)
    except subprocess.CalledProcessError as e:
        output = e.output
        success = False

    output = output.decode("utf-8")
    if not success:
        raise Exception("Failed to run %s" % ",".join(cmd))

    return output
