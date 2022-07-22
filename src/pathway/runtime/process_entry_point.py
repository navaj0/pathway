import argparse
import boto3
import pickle
import inspect

from pathway.runtime.bootstrap import bootstrap

from .dataset import Input, Output, _PickleDataLoader, is_primitive, Timer, parse_s3_url

from typing import Callable
from urllib.parse import urlparse


def processing_script(input_args):
    # limits on container arguments
    # https://code.amazon.com/packages/IronmanApiServiceModel/blobs/269d344ed88965188aecc541b99e69d02deb839f/--/model/processing/types/common-types.xml#L620

    parser = argparse.ArgumentParser()
    parser.add_argument('--func-code', type=str)
    parser.add_argument('--env-def', type=str)
    args, _ = parser.parse_known_args(input_args)

    # bootstrap
    if args.env_def:
        bootstrap(args.env_def)

    # deserialize the code
    func = _get_function(args.func_code)

    # rebuild the arguments
    signature = inspect.signature(func)
    parameters = signature.parameters
    for key in parameters.keys():
        if parameters.get(key).annotation == Input:
            parser.add_argument(f"--{key}", type=str)
        elif parameters.get(key).annotation == Output:
            parser.add_argument(f"--{key}", type=str)
        elif parameters.get(key).annotation in (int, float, bool, str):
            parser.add_argument(f"--{key}",
                                type=parameters.get(key).annotation,
                                default=parameters.get(key).default)
        else:
            parser.add_argument(f"--{key}", type=str)


    if signature.return_annotation is not inspect._empty:
        parser.add_argument(f'--return', type=str)

    args, _ = parser.parse_known_args(input_args)

    call_args = {}
    for key in parameters.keys():
        if parameters.get(key).annotation == Input:
            call_args[key] = Input(getattr(args, key))
        elif parameters.get(key).annotation == Output:
            call_args[key] = Output(getattr(args, key))
        elif is_primitive(parameters.get(key).annotation):
            call_args[key] = getattr(args, key)
        else:
            call_args[key] = _PickleDataLoader.load_from_s3(getattr(args, key))

    # invoke
    results = func(**call_args)

    if signature.return_annotation is not inspect._empty:
        _PickleDataLoader.save_to_s3(results, getattr(args, 'return'))


def _get_function(code_location: str) -> Callable:
    with Timer(name=f"loading code from {code_location}"):
        bucket, object_key = parse_s3_url(code_location)

        session = boto3.session.Session()
        s3_resource = session.resource('s3')

        code = s3_resource.Object(bucket, object_key).get()['Body'].read()
        return pickle.loads(code)

