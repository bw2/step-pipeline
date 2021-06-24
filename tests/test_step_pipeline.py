import configargparse
import mock
import unittest
from step_pipeline import pipeline
from tests.test_utils import PipelineTest


class Test(unittest.TestCase):

    def setUp(self) -> None:
        p = configargparse.getParser()
        self._pipeline = PipelineTest(p, name="test_pipeline")

    #def test_(self):
    #    test_step = pipeline._Step(self._pipeline, "test_step1")
