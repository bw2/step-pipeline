import configargparse
import hail as hl
import os
import unittest

from step_pipeline.pipeline import _Pipeline, _Step
from step_pipeline.io import Localize, Delocalize
from step_pipeline.utils import check_gcloud_storage_region, _GoogleStorageException, \
    _path_exists__cached, _file_stat__cached, _generate_gs_path_to_file_stat_dict, are_any_inputs_missing, \
    are_outputs_up_to_date

hl.init(log="/dev/null", quiet=True)

HG38_PATH = "gs://gcp-public-data--broad-references/hg38/v0/Homo_sapiens_assembly38.fasta"
HG38_PATH_WITH_STAR = "gs://gcp-public-data--broad-references/hg38/v0/Homo_sapiens_*ssembly38.fasta"
HG38_DBSNP_PATH = "gs://gcp-public-data--broad-references/hg38/v0/Homo_sapiens_assembly38.dbsnp138.vcf.gz"
HG38_DBSNP_PATH_WITH_STAR = f"{HG38_DBSNP_PATH}*"
ACCESS_DENIED_PATH = "gs://test/access_denied"


class PipelineTest(_Pipeline):
    """Subclass _Pipeline to override the abstract methods so it can be instanciated."""

    def run(self):
        pass

    def new_step(self, short_name, step_number=None):
        pass

    def _get_localization_root_dir(self, localize_by):
        return "/"


class StepWithSupportForCopy(_Step):
    def _get_supported_localization_strategies(self):
        return {
            Localize.HAIL_HADOOP_COPY,
            Localize.COPY,
        }
    def _get_supported_delocalization_strategies(self):
        return {
            Delocalize.COPY,
        }
    def _preprocess_input(self, input_spec):
        pass
    def _preprocess_output(self, output_spec):
        pass
    def _transfer_input(self, input_spec):
        pass
    def _transfer_output(self, output_spec):
        pass


class Test(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None
        self._pipeline = PipelineTest(name="test_pipeline")

    def test_generate_gs_path_to_file_stat_dict(self):
        self.assertRaisesRegex(
            ValueError,
            "doesn't start with gs://",
            _generate_gs_path_to_file_stat_dict,
            "/dir/file.txt"
        )

        self.assertRaisesRegex(
            ValueError,
            "Unexpected argument type ",
            _generate_gs_path_to_file_stat_dict,
            ["/dir/file.txt"],
        )

        paths = _generate_gs_path_to_file_stat_dict("gs://missing-path")
        self.assertEqual(len(paths), 0)

        hg38_path = "gs://gcp-public-data--broad-references/hg38/v0/Homo_sapiens_*ssembly38.fasta"
        paths = _generate_gs_path_to_file_stat_dict(hg38_path)
        self.assertEqual(len(paths), 1)
        for path, metadata in paths.items():
            self.assertEqual(path, hg38_path.replace("*", "a"))

        for i in range(2):  # run 2x to test caching
            paths = _generate_gs_path_to_file_stat_dict(HG38_DBSNP_PATH_WITH_STAR)
            self.assertEqual(len(paths), 2)

            items_iter = iter(sorted(paths.items()))
            path, metadata = next(items_iter)
            self.assertEqual(path, HG38_DBSNP_PATH)

            path, metadata = next(items_iter)
            self.assertEqual(path, f"{HG38_DBSNP_PATH}.tbi")

    def test_path_exists__cached(self):
        self.assertRaisesRegex(
            ValueError,
            "Unexpected path type ",
            _path_exists__cached,
            ["/dir/file.txt"],
        )

        for i in range(2):  # run 2x to test caching
            self.assertTrue(
                _path_exists__cached(HG38_PATH_WITH_STAR))
            self.assertFalse(
                _path_exists__cached("gs://gcp-public-data--broad-references/hg38/v0/Homo_sapiens_assembly100.fasta"))
            self.assertFalse(
                _path_exists__cached("gs://missing-bucket"))

    def test_file_stat__cached(self):
        hg38_stat_expected_results = {'path': HG38_PATH, 'size_bytes': 3249912778}

        # test gs:// paths
        for i in range(2):  # run 2x to test caching
            for path in HG38_PATH_WITH_STAR, HG38_PATH:
                for stat in _file_stat__cached(path):
                    self.assertTrue(len({"path", "size_bytes", "modification_time"} - set(stat.keys())) == 0)
                    self.assertEqual(stat["path"], hg38_stat_expected_results["path"])
                    self.assertEqual(stat["size_bytes"], hg38_stat_expected_results["size_bytes"])

            self.assertRaises(
                FileNotFoundError,
                _file_stat__cached,
                "gs://gcp-public-data--broad-references/hg38/v0/Homo_sapiens_assembly100.fasta",
            )
            self.assertRaises(
                FileNotFoundError,
                _file_stat__cached,
                "/missing-dir/path",
            )

        # test local paths
        for i in range(2):  # run 2x to test caching
            for path in (
                    "README.md",
                    os.path.abspath("tests/__init__.py"),
            ):
                for stat in _file_stat__cached(path):
                    self.assertTrue(len({"path", "size_bytes", "modification_time"} - set(stat.keys())) == 0)
                    self.assertEqual(stat["path"], path)

            self.assertRaises(
                FileNotFoundError,
                _file_stat__cached,
                "/data/missing file",
            )

    def test_are_any_inputs_missing(self):
        test_step = StepWithSupportForCopy(self._pipeline, "test_step")
        self.assertFalse(are_any_inputs_missing(test_step))

        test_step.input("some_file.txt", localize_by=Localize.COPY)
        self.assertTrue(are_any_inputs_missing(test_step, verbose=True))

        test_step2 = StepWithSupportForCopy(self._pipeline, "test_step")
        test_step2.input("README.md", localize_by=Localize.COPY)
        input_spec = test_step2.input("LICENSE", localize_by=Localize.COPY)
        self.assertFalse(are_any_inputs_missing(test_step2))
        self.assertDictEqual(input_spec.__dict__,
             {
                 '_source_path': 'LICENSE',
                 '_localize_by': Localize.COPY,
                 '_source_path_without_protocol': 'LICENSE',
                 '_filename': 'LICENSE',
                 '_source_bucket': None,
                 '_source_dir': '',
                 '_local_dir': '/local_copy/',
                 '_local_path': '/local_copy/LICENSE',
                 '_name': 'LICENSE',
             })

        source_path = os.path.abspath("tests/__init__.py")
        test_step3 = StepWithSupportForCopy(self._pipeline, "test_step")
        input_spec = test_step3.input(
            source_path,
            name="test_input_name",
            localize_by=Localize.COPY,
        )
        self.assertDictEqual(input_spec.__dict__,
             {
                 '_source_path': source_path,
                 '_source_bucket': None,
                 '_localize_by': Localize.COPY,
                 '_source_path_without_protocol': source_path,
                 '_filename': os.path.basename(source_path),
                 '_source_dir': os.path.dirname(source_path),
                 '_local_dir': '/local_copy' + os.path.dirname(source_path),
                 '_local_path': '/local_copy' + source_path,
                 '_name': 'test_input_name',
             })

    def test_are_outputs_up_to_date(self):
        test_step = StepWithSupportForCopy(self._pipeline, "test_step")
        self.assertFalse(are_outputs_up_to_date(test_step))

        for localize_by in (
                Localize.GSUTIL_COPY,
                Localize.HAIL_BATCH_GCSFUSE,
                Localize.HAIL_BATCH_GCSFUSE_VIA_TEMP_BUCKET):
            self.assertRaisesRegex(ValueError, "doesn't start with gs://", test_step.input,
                "some_file.txt", localize_by=localize_by)


        # test missing input path
        test_step = StepWithSupportForCopy(self._pipeline, "test_step")
        test_step.input("gs://missing-bucket/test", localize_by=Localize.COPY)
        test_step.output(HG38_PATH, HG38_PATH, delocalize_by=Delocalize.COPY)
        self.assertRaisesRegex(ValueError, "missing", are_outputs_up_to_date,
            test_step, verbose=True)

        # test missing output path
        test_step = StepWithSupportForCopy(self._pipeline, "test_step")
        test_step.input(HG38_PATH_WITH_STAR, localize_by=Localize.COPY)
        test_step.output(HG38_PATH, "gs//missing-bucket", delocalize_by=Delocalize.COPY)
        self.assertFalse(are_outputs_up_to_date(test_step, verbose=True))

        # test regular paths which exist and are up-to-date
        test_step = StepWithSupportForCopy(self._pipeline, "test_step")
        test_step.input(HG38_PATH, localize_by=Localize.COPY)
        test_step.output(HG38_PATH, HG38_PATH, delocalize_by=Delocalize.COPY)
        self.assertTrue(are_outputs_up_to_date(test_step, verbose=True))

        # test glob paths
        test_step.input(HG38_PATH_WITH_STAR, localize_by=Localize.COPY)
        self.assertTrue(are_outputs_up_to_date(test_step, verbose=True))

        # add output which is newer than all inputs
        test_step.output(HG38_DBSNP_PATH, output_dir=os.path.dirname(HG38_DBSNP_PATH), delocalize_by=Delocalize.COPY)
        self.assertTrue(are_outputs_up_to_date(test_step))

        # add input which is newer than some outputs
        test_step.input(HG38_DBSNP_PATH, localize_by=Localize.COPY)
        self.assertFalse(are_outputs_up_to_date(test_step))

        # add output which is older
        test_step.output(HG38_DBSNP_PATH, output_dir=os.path.dirname(HG38_DBSNP_PATH), delocalize_by=Delocalize.COPY)
        self.assertFalse(are_outputs_up_to_date(test_step))

    def test_check_gcloud_storage_region(self):
        self.assertRaisesRegex(
            _GoogleStorageException, "Access denied",
            check_gcloud_storage_region,
            "gs://test/access-denied",
            expected_regions=("US"),
            ignore_access_denied_exception=False,
        )

        check_gcloud_storage_region("gs://test/access-denied", expected_regions=("US"),
            ignore_access_denied_exception=True,
        )

        self.assertIsNone(
            check_gcloud_storage_region(
                HG38_PATH,
                expected_regions=("US"),
                ignore_access_denied_exception=True,
            ))

        self.assertRaisesRegex(
            _GoogleStorageException, "is located in US-CENTRAL1",
            check_gcloud_storage_region,
            "gs://seqr-reference-data/GRCh38/1kg/1kg.wgs.phase3.20170504.GRCh38_sites.vcf.gz",
            expected_regions=("US"),
        )

        self.assertIsNone(
            check_gcloud_storage_region(
                "gs://seqr-reference-data/GRCh38/1kg/1kg.wgs.phase3.20170504.GRCh38_sites.vcf.gz",
                expected_regions=("US", "US-CENTRAL1"))
        )
