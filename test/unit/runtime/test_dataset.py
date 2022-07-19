from pathway.runtime.dataset import Job, DataObject, _AsyncDataObject, is_data_object
from typing import Dict

import unittest


class MockJob(Job):
    def is_completed(self):
        return True

    def wait(self):
        pass

    def stop(self):
        pass


class JobTestCase(unittest.TestCase):
    def test_no_return_type(self):
        def func():
            pass
        job = MockJob(func=func, job_name='job_name')
        self.assertIsNone(job.results())

    def test_return_data_object(self):
        def func() -> DataObject[Dict]:
            pass
        job = MockJob(func=func, job_name='job_name')

        self.assertTrue(isinstance(job.results(), DataObject))

    def test_return_data_object_2(self):
        def func() -> DataObject:
            pass
        job = MockJob(func=func, job_name='job_name')

        self.assertTrue(isinstance(job.results(), DataObject))


class IsDataObjectTest(unittest.TestCase):
    def test(self):
        self.assertTrue(is_data_object(DataObject))

    def test_subclass(self):
        self.assertTrue(is_data_object(_AsyncDataObject))


if __name__ == '__main__':
    unittest.main()
