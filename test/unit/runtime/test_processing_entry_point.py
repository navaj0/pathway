import unittest

from pathway.runtime.process_entry_point import processing_script
from pathway.runtime.dataset import DataObject
from unittest.mock import Mock, patch


class ProcessingEntryPointTestCase(unittest.TestCase):

    def test_func(self):
        func_mock = Mock()

        def func(x: float):
            func_mock(x=x)

        with patch('pathway.runtime.process_entry_point._get_function', return_value=func):
            script_arguments = ['--func-code', 's3://func', '--x', '0.0']
            processing_script(script_arguments)
            func_mock.assert_called_once_with(x=0.0)

    def test_func_data_object(self):
        returned_object = DataObject.set(4)
        func_mock = Mock()
        func_mock.return_value = returned_object

        def func(x: DataObject) -> DataObject:
            return func_mock(x=x)

        with patch('pathway.runtime.process_entry_point._get_function', return_value=func):
            with patch('pathway.runtime.dataset._PickleDataLoader.load_from_s3') as mock_load_from_s3:
                with patch('pathway.runtime.dataset._PickleDataLoader.save_to_s3') as mock_save_to_s3:
                    mock_load_from_s3.return_value = 3

                    script_arguments = [
                        '--func-code', 's3://func',
                        '--x', '/opt/data',
                        '--return', '/opt/return'
                    ]
                    processing_script(script_arguments)

                    func_mock.assert_called_once_with(x=3)
                    mock_save_to_s3.assert_called_once_with(4, '/opt/return')


if __name__ == '__main__':
    unittest.main()
