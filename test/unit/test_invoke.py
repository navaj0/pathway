import unittest
from typing import Callable, Any, Union
from unittest.mock import patch, ANY, Mock

from pathway.invoke import run_processing_job
from pathway.runtime.dataset import Input, Output, DataObject, _AsyncDataObject


@patch('pathway.invoke.Processor')
@patch('pathway.invoke.pickle_func', return_value='s3://func')
class ProcessingJobTestCase(unittest.TestCase):
    def test_run_function_as_processing_job(self, pickle_func_mock, processor_mock):
        def func(data: Input, output: Output, split_ratio: float):
            pass

        base_image = 'base_image'
        run_processing_job(
            image_uri=base_image,
            instance_type='ml.m5.large',
            environment_definition=None,
            func=func,
            arguments_dict={
                'data': Input('s3://input.csv'),
                'output': Output('s3://output.csv'),
                'split_ratio': 0.7
            })

        processor_mock.assert_called_once_with(
            image_uri=base_image,
            instance_type='ml.m5.large',
            instance_count=1,
            role="arn:aws:iam::789267064511:role/service-role/AmazonSageMaker-ExecutionRole-20200403T112353",
            entrypoint=['pathway-runtime'],
            sagemaker_session=ANY
        )

        processor_mock.return_value.run.assert_called_once_with(
            arguments=['--func-code', 's3://func',
                       '--data', 's3://input.csv',
                       '--output', 's3://output.csv',
                       '--split_ratio', '0.7'],
            inputs=ANY,
            outputs=ANY,
            job_name=ANY,
            wait=False,
            logs=False
        )

    def test_run_function_with_data_objects(self, pickle_func_mock, processor_mock):

        def func(input_data: DataObject) -> DataObject:
            pass

        base_image = 'base_image'
        input_data = DataObject.set(0.3)

        with patch('pathway.runtime.dataset._PickleDataLoader.save_to_s3') as mock_save_to_s3:
            job = run_processing_job(
                image_uri=base_image,
                instance_type='ml.m5.large',
                environment_definition=None,
                func=func,
                arguments_dict={
                    'input_data': input_data,
                })

            self.assertTrue(isinstance(job.results, _AsyncDataObject))

            processor_mock.assert_called_once_with(
                image_uri=base_image,
                instance_type='ml.m5.large',
                instance_count=1,
                role="arn:aws:iam::789267064511:role/service-role/AmazonSageMaker-ExecutionRole-20200403T112353",
                entrypoint=['pathway-runtime'],
                sagemaker_session=ANY
            )

            processor_mock.return_value.run.assert_called_once_with(
                arguments=['--func-code', 's3://func',
                           '--input_data', '/opt/ml/processing/inputs/input_data/input_data.pkl',
                           '--return', '/opt/ml/processing/outputs/return.pkl'
                           ],
                inputs=ANY,
                outputs=ANY,
                job_name=ANY,
                wait=False,
                logs=False
            )

        mock_save_to_s3.assert_called_once()

    def test_run_function_chain_jobs(self, pickle_func_mock, processor_mock):

        def func(input_data: DataObject) -> DataObject:
            pass

        base_image = 'base_image'
        input_data = _AsyncDataObject(job=Mock(), path="s3://first-job/input.pkl")

        with patch('pathway.runtime.dataset._PickleDataLoader.save_to_s3') as mock_save_to_s3:
            job = run_processing_job(
                image_uri=base_image,
                instance_type='ml.m5.large',
                func=func,
                environment_definition=None,
                arguments_dict={
                    'input_data': input_data,
                })

            self.assertTrue(isinstance(job.results, _AsyncDataObject))

            processor_mock.assert_called_once_with(
                image_uri=base_image,
                instance_type='ml.m5.large',
                instance_count=1,
                role="arn:aws:iam::789267064511:role/service-role/AmazonSageMaker-ExecutionRole-20200403T112353",
                entrypoint=['pathway-runtime'],
                sagemaker_session=ANY
            )

            processor_mock.return_value.run.assert_called_once_with(
                arguments=['--func-code', 's3://func',
                           '--input_data', 's3://first-job/input.pkl',
                           '--return', '/opt/ml/processing/outputs/return.pkl'
                           ],
                inputs=ANY,
                outputs=ANY,
                job_name=ANY,
                wait=False,
                logs=False
            )

        mock_save_to_s3.assert_not_called()


if __name__ == '__main__':
    unittest.main()
