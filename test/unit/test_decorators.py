import unittest
from collections import OrderedDict
from unittest.mock import patch, ANY
from pathway.runtime.dataset import Input, Output
from pathway.decorators import processing_job


class ProcessingJobTestCase(unittest.TestCase):
    @patch('pathway.invoke.run_processing_job')
    def test_processing_job(self, processing_job_runner):
        @processing_job(base_image='base_image')
        def func(data: Input, output: Output, split_ratio: float):
            pass

        func(Input('s3://input'), Output('s3://output'), split_ratio=0.7)

        processing_job_runner.assert_called_with('base_image', func=ANY, arguments_dict=OrderedDict([
            ('data', Input('s3://input')),
            ('output', Output('s3://output')),
            ('split_ratio', 0.7)
        ]))

    @patch('pathway.invoke.run_processing_job')
    def test_raise_on_missing_arguments(self, processing_job_runner):
        @processing_job(base_image='base_image')
        def func(data: Input, output: Output, split_ratio: float):
            pass

        with self.assertRaises(TypeError):
            func(Input('s3://input'), Output('s3://output'))

    @patch('pathway.invoke.run_processing_job')
    def test_missing_argument_with_default_value(self, processing_job_runner):
        @processing_job(base_image='base_image')
        def func(data: Input, output: Output, split_ratio: float = 0.7):
            pass

        func(Input('s3://input'), Output('s3://output'))

        processing_job_runner.assert_called_with('base_image', func=ANY, arguments_dict=OrderedDict([
            ('data', Input('s3://input')),
            ('output', Output('s3://output'))
        ]))

    @patch('pathway.invoke.run_processing_job')
    def test_decorate_used_multiple_times(self, processing_job_runner):
        @processing_job(base_image='base_image_1')
        @processing_job(base_image='base_image_2')
        def func(data: Input, output: Output, split_ratio: float = 0.7):
            pass

        func(Input('s3://input'), Output('s3://output'))

        processing_job_runner.assert_called_once_with('base_image_1', func=ANY, arguments_dict=OrderedDict([
            ('data', Input('s3://input')),
            ('output', Output('s3://output'))
        ]))


if __name__ == '__main__':
    unittest.main()
