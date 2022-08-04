import boto3
from pathway.runtime.dataset import Input, Output, DataObject, _AsyncDataObject, Job, _PickleDataLoader
from sagemaker.processing import Processor, ProcessingInput, ProcessingOutput
from sagemaker.session import Session
from sagemaker.workflow.pipeline_context import PipelineSession
from sagemaker.workflow.steps import ProcessingStep
from typing import Callable, Dict, List, get_type_hints, Optional
from .util import pickle_func, sagemaker_timestamp, upload_code_package
from .pipeline import PipelineContext

import os


class ProcessingJob(Job):
    def __init__(self, job_name: str, sagemaker_session: Session, results=None):
        super().__init__(results)
        self._sagemaker_session = sagemaker_session
        self._job_name = job_name
        self._is_completed = False

    def wait(self, logs=True):
        """Waits for the processing job to complete.

        Args:
            logs (bool): Whether to show the logs produced by the job (default: True).

        """
        if logs:
            self._sagemaker_session.logs_for_processing_job(self._job_name, wait=True)
        else:
            self._sagemaker_session.wait_for_processing_job(self._job_name)

    def describe(self):
        """Prints out a response from the DescribeProcessingJob API call."""
        return self._sagemaker_session.describe_processing_job(self._job_name)

    def stop(self):
        """Stops the processing job."""
        self._sagemaker_session.stop_processing_job(self._job_name)

    def is_completed(self):
        if not self._is_completed:
            describe_response = self._sagemaker_session.describe_processing_job(self._job_name)
            self._is_completed = describe_response['ProcessingJobStatus'] == 'Completed'
        return self._is_completed


def run_processing_job(image_uri: str,
                       instance_type: str,
                       instance_count: int,
                       environment_definition: Optional[str],
                       func: Callable,
                       arguments_dict: Dict):

    boto3_session = boto3.session.Session(region_name='us-east-2')

    if PipelineContext.get_current_pipeline_session():
        sagemaker_session = PipelineSession(boto_session=boto3_session)
    else:
        sagemaker_session = Session(boto_session=boto3_session)

    base_job_name = func.__name__.replace('_', '-')
    job_name = f"{base_job_name}-{sagemaker_timestamp()}"

    code_location = pickle_func(func, boto3_session, sagemaker_session.default_bucket(), job_name)
    command = ['--func-code', code_location]

    if environment_definition:
        location = upload_code_package(environment_definition, boto3_session,
                                       bucket=sagemaker_session.default_bucket(),
                                       s3_key_prefix=job_name)
        command.append('--env-def')
        command.append(location)

    entry_point = ['pathway-runtime']

    processor_inputs = []
    processor_outputs = []

    base_dir = "/opt/ml/processing"
    for key, value in arguments_dict.items():
        print(f"{key}, {value}")
        if isinstance(value, Input):
            command.append(f"--{key}")
            command.append(value.path)
        elif isinstance(value, Output):
            command.append(f"--{key}")
            command.append(value.path)
        elif type(value) in (int, str, float, bool):
            command.append(f"--{key}")
            command.append(str(value))
        elif isinstance(value, _AsyncDataObject):
            command.append(f"--{key}")
            command.append(value._path)
        else:
            # TODO: should we do this for a pipeline?
            s3_uri = f"s3://{sagemaker_session.default_bucket()}/{job_name}/{key}.pkl"
            # TODO: how to avoid accessing the protected method?
            data = value.get() if isinstance(value, DataObject) else value
            _PickleDataLoader.save_to_s3(data, s3_uri=s3_uri)
            command.append(f"--{key}")
            command.append(s3_uri)

    # build the outputs
    type_hints = get_type_hints(func)
    if 'return' not in type_hints.keys():
        pass
    else:
        command.append("--return")
        command.append(f"s3://{sagemaker_session.default_bucket()}/{job_name}/outputs/return.pkl")

    _processor = Processor(
        image_uri=image_uri,
        instance_type=instance_type,
        instance_count=instance_count,
        role="arn:aws:iam::789267064511:role/service-role/AmazonSageMaker-ExecutionRole-20200403T112353",
        # entrypoint=entry_point,
        sagemaker_session=sagemaker_session
    )

    if PipelineContext.get_current_pipeline_session():
        step = ProcessingStep(
            name=func.__name__,
            step_args=_processor.run(
                arguments=command,
                inputs=processor_inputs,
                outputs=processor_outputs
            ))
        PipelineContext.get_current_pipeline_session().append_step(step)
        return step
    else:
        _processor.run(arguments=command,
                       inputs=processor_inputs,
                       outputs=processor_outputs,
                       job_name=job_name,
                       wait=False,
                       logs=False)
        job = ProcessingJob(sagemaker_session=sagemaker_session, job_name=job_name)
        # build the outputs
        type_hints = get_type_hints(func)
        if 'return' not in type_hints.keys():
            results = None
        else:
            results = _AsyncDataObject(
                job, path=f"s3://{sagemaker_session.default_bucket()}/{job_name}/outputs/return.pkl")
        job._results = results
        return job


def run_training_job(func: Callable, func_args: Dict, image_uri: str):

    from sagemaker import LocalSession
    from sagemaker.estimator import Estimator, TrainingInput

    sagemaker_session = LocalSession()

    # Getting the argument names of the called function
    keys = func.__code__.co_varnames[:func.__code__.co_argcount]

    training_inputs = {}

    for key, value in zip(keys, func_args):
        if isinstance(value, DataSet):
            training_inputs[key] = TrainingInput(s3_data=value.path)
        else:
            raise ValueError

    _estimator = Estimator(
        image_uri=image_uri,
        instance_type='local',
        instance_count=1,
        base_job_name="cal-housing",
        role="arn:aws:iam::789267064511:role/service-role/AmazonSageMaker-ExecutionRole-20200403T112353",
        sagemaker_session=sagemaker_session
    )

    _estimator.fit(training_inputs)

