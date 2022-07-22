from typing import Callable, List
from functools import wraps


def processing_job(image_uri: str,
                   instance_type: str,
                   environment_definition: str = None):

    def inner(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            from inspect import signature
            from .invoke import run_processing_job

            # Getting the argument names of the
            # called function
            func_signature = signature(func)
            argument_binding = func_signature.bind(*args, **kwargs)
            arguments_dict = argument_binding.arguments

            return run_processing_job(image_uri,
                                      instance_type=instance_type,
                                      environment_definition=environment_definition,
                                      func=func,
                                      arguments_dict=arguments_dict)
        return wrapper

    return inner
