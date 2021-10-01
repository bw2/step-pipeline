import configargparse
import hail as hl
import os
import sys
import unittest
import unittest.mock

import step_pipeline
from step_pipeline import batch
from step_pipeline import pipeline
from step_pipeline import utils
from step_pipeline.pipeline import LocalizationStrategy, DelocalizationStrategy
from step_pipeline.utils import check_gcloud_storage_region, _GoogleStorageException, \
    _file_exists__cached, _file_stat__cached, _generate_gs_path_to_file_stat_dict

"""
class TestPipeline(unittest.TestCase):

    def setUp(self):
        p = configargparse.getParser()

        patcher = unittest.mock.patch('step_pipeline.batch._BatchPipeline._create_batch_obj')
        self.addCleanup(patcher.stop)
        self._create_batch_obj_mock = patcher.start()

        patcher = unittest.mock.patch('step_pipeline.batch._BatchPipeline._run_batch_obj')
        self.addCleanup(patcher.stop)
        self._run_batch_obj = patcher.start()

        self._pipeline = batch._BatchPipeline(p, name="test_pipeline")

    def test_new_step(self):

        step1 = self._pipeline.new_step("test_step")
        self.assertIsNotNone(step1)

        step2 = self._pipeline.new_step(
            "test_step",
            step_number=1,
            image="docker image sha256",
            cpu=3,
            memory=5,
            storage=10,
            always_run=True,
            timeout=False,
            write_commands_to_script=True,
            save_script_to_output_dir=True,
            profile_cpu_memory_and_disk_usage=True,
            reuse_job_from_previous_step=step1,
        )

        step2.depends_on(step1)

    @unittest.mock.patch("step_pipeline.batch._BatchPipeline._transfer_all_steps")
    def test_run(self, transfer_all_steps):
        self._pipeline._run()
"""