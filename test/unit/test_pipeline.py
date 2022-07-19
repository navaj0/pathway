import unittest

from pathway.pipeline import PipelineContext, PipelineSession


class MyTestCase(unittest.TestCase):
    def test_session_registered(self):
        with PipelineSession() as session:
            self.assertIsNotNone(PipelineContext.get_current_pipeline_session())
            self.assertEquals(session, PipelineContext.get_current_pipeline_session())
        self.assertIsNone(PipelineContext.get_current_pipeline_session())

    def test_raise(self):
        try:
            with PipelineSession() as session:
                raise ValueError
        except ValueError:
            pass

        self.assertIsNone(PipelineContext.get_current_pipeline_session())


if __name__ == '__main__':
    unittest.main()
