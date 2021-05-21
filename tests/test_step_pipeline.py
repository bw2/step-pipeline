import configargparse
import unittest
import step_pipeline

class TestPipeline(step_pipeline._Pipeline):

    def _run(self):
        pass

    def new_step(self, short_name, step_number=None):
        pass


class Test(unittest.TestCase):

    def setUp(self) -> None:
        p = configargparse.getParser()
        self._pipeline = TestPipeline(p, name="test_pipeline")

    def test_are_outputs_up_to_date(self):
        test_step = step_pipeline._Step(self._pipeline, "test_step1")
        self.assertFalse(step_pipeline.are_outputs_up_to_date(test_step))
