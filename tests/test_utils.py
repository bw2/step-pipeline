import configargparse
import hail as hl
import os
import unittest
from step_pipeline import pipeline
from step_pipeline import utils
from step_pipeline.pipeline import LocalizationStrategy, DelocalizationStrategy
from step_pipeline.utils import check_gcloud_storage_region, _GoogleStorageException, \
    _file_exists__cached, _file_stat__cached, _generate_gs_path_to_file_stat_dict

hl.init(log="/dev/null")

HG38_PATH = "gs://gcp-public-data--broad-references/hg38/v0/Homo_sapiens_assembly38.fasta"
HG38_PATH_WITH_STAR = "gs://gcp-public-data--broad-references/hg38/v0/Homo_sapiens_*ssembly38.fasta"
HG38_DBSNP_PATH = "gs://gcp-public-data--broad-references/hg38/v0/Homo_sapiens_assembly38.dbsnp138.vcf.gz"
HG38_DBSNP_PATH_WITH_STAR = f"{HG38_DBSNP_PATH}*"


class PipelineTest(pipeline._Pipeline):
    """Subclass _Pipeline to override all abstract methods so that it can be instanciated."""

    def run(self):
        pass

    def new_step(self, short_name, step_number=None):
        pass


class StepWithSupportForCopy(pipeline._Step):
    def _get_supported_localization_strategies(self):
        return {
            LocalizationStrategy.HAIL_HADOOP_COPY,
            LocalizationStrategy.COPY,
        }

    def _get_supported_delocalization_strategies(self):
        return {
            DelocalizationStrategy.COPY,
        }


class Test(unittest.TestCase):

    def setUp(self) -> None:
        p = configargparse.getParser()
        self._pipeline = PipelineTest(p, name="test_pipeline")

    def test__generate_gs_path_to_file_stat_dict(self):
        self.assertRaisesRegex(
            ValueError,
            "doesn't start with gs://",
            _generate_gs_path_to_file_stat_dict,
            "/dir/file.txt"
        )

        self.assertRaisesRegex(
            ValueError,
            "Unexpected glob_path type ",
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

    def test__file_exists__cached(self):
        self.assertRaisesRegex(
            ValueError,
            "Unexpected path type ",
            _file_exists__cached,
            ["/dir/file.txt"],
        )

        for i in range(2):  # run 2x to test caching
            self.assertTrue(
                _file_exists__cached(HG38_PATH_WITH_STAR))
            self.assertFalse(
                _file_exists__cached("gs://gcp-public-data--broad-references/hg38/v0/Homo_sapiens_assembly100.fasta"))
            self.assertFalse(
                _file_exists__cached("gs://missing-bucket"))

    def test__file_stat__cached(self):
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
        self.assertFalse(utils.are_any_inputs_missing(test_step))

        test_step.input("some_file.txt", localization_strategy=LocalizationStrategy.COPY)
        self.assertTrue(utils.are_any_inputs_missing(test_step, verbose=True))

        test_step2 = StepWithSupportForCopy(self._pipeline, "test_step")
        test_step2.input("README.md", localization_strategy=LocalizationStrategy.COPY)
        input_spec = test_step2.input("LICENSE", localization_strategy=LocalizationStrategy.COPY)
        self.assertDictEqual(input_spec,
             {
                 'source_path': 'LICENSE',
                 'destination_root_dir': '/',
                 'localization_strategy': LocalizationStrategy.COPY,
                 'source_path_without_protocol': 'LICENSE',
                 'filename': 'LICENSE',
                 'source_dir': '',
                 'local_dir': '/localized/',
                 'local_path': '/localized/LICENSE',
                 'name': 'LICENSE',
             })
        self.assertFalse(utils.are_any_inputs_missing(test_step2))

        source_path = os.path.abspath("tests/__init__.py")
        test_step3 = StepWithSupportForCopy(self._pipeline, "test_step")
        input_spec = test_step3.input(
            source_path,
            destination_root_dir=".",
            name="test_input_name",
            localization_strategy=LocalizationStrategy.COPY,
        )
        self.assertDictEqual(input_spec,
             {
                 'source_path': source_path,
                 'destination_root_dir': '.',
                 'localization_strategy': LocalizationStrategy.COPY,
                 'source_path_without_protocol': source_path,
                 'filename': os.path.basename(source_path),
                 'source_dir': os.path.dirname(source_path),
                 'local_dir': './localized' + os.path.dirname(source_path),
                 'local_path': './localized' + source_path,
                 'name': 'test_input_name',
             })

    def test_are_outputs_up_to_date(self):
        test_step = pipeline._Step(self._pipeline, "test_step")
        self.assertFalse(utils.are_outputs_up_to_date(test_step))

        self.assertRaisesRegex(ValueError, "Unsupported", test_step.input,
            "some_file.txt", localization_strategy=pipeline.LocalizationStrategy.COPY)

        for localization_strategy in (
                pipeline.LocalizationStrategy.GSUTIL_COPY,
                pipeline.LocalizationStrategy.HAIL_BATCH_GCSFUSE,
                pipeline.LocalizationStrategy.HAIL_BATCH_GCSFUSE_VIA_TEMP_BUCKET):
            self.assertRaisesRegex(ValueError, "doesn't start with gs://", test_step.input,
                "some_file.txt", localization_strategy=localization_strategy)


        # test missing input path
        test_step = StepWithSupportForCopy(self._pipeline, "test_step")
        test_step.input("gs://missing-bucket/test", localization_strategy=LocalizationStrategy.COPY)
        test_step.output(HG38_PATH, HG38_PATH, delocalization_strategy=DelocalizationStrategy.COPY)
        self.assertRaisesRegex(ValueError, "missing", utils.are_outputs_up_to_date,
            test_step, verbose=True)

        # test missing output path
        test_step = StepWithSupportForCopy(self._pipeline, "test_step")
        test_step.input(HG38_PATH_WITH_STAR, localization_strategy=LocalizationStrategy.COPY)
        test_step.output(HG38_PATH, "gs//missing-bucket") #, delocalization_strategy=DelocalizationStrategy.COPY)
        self.assertFalse(utils.are_outputs_up_to_date(test_step, verbose=True))

        # test regular paths which exist and are up-to-date
        test_step = StepWithSupportForCopy(self._pipeline, "test_step")
        test_step.input(HG38_PATH, localization_strategy=LocalizationStrategy.COPY)
        test_step.output(HG38_PATH, HG38_PATH, delocalization_strategy=DelocalizationStrategy.COPY)
        self.assertTrue(utils.are_outputs_up_to_date(test_step, verbose=True))

        # test glob paths
        test_step.input(HG38_PATH_WITH_STAR, localization_strategy=LocalizationStrategy.COPY)
        self.assertTrue(utils.are_outputs_up_to_date(test_step, verbose=True))

        # add output which is newer than all inputs
        test_step.output(HG38_DBSNP_PATH, destination_dir=os.path.dirname(HG38_DBSNP_PATH), delocalization_strategy=DelocalizationStrategy.COPY)
        self.assertTrue(utils.are_outputs_up_to_date(test_step))

        # add input which is newer than some outputs
        test_step.input(HG38_DBSNP_PATH, localization_strategy=LocalizationStrategy.COPY)
        self.assertFalse(utils.are_outputs_up_to_date(test_step))

        # add output which is older
        test_step.output(HG38_DBSNP_PATH, destination_dir=os.path.dirname(HG38_DBSNP_PATH), delocalization_strategy=DelocalizationStrategy.COPY)
        self.assertFalse(utils.are_outputs_up_to_date(test_step))

    def test_check_gcloud_storage_region(self):
        self.assertRaisesRegex(
            _GoogleStorageException, "Couldn't determine",
            check_gcloud_storage_region,
            "gs://imaginary-bucket",
            expected_regions=("US"),
            ignore_access_denied_exception=False,
        )

        self.assertRaisesRegex(
            _GoogleStorageException, "Access denied",
            check_gcloud_storage_region,
            HG38_PATH_WITH_STAR,
            expected_regions=("US"),
            ignore_access_denied_exception=False,
        )

        self.assertIsNone(
            check_gcloud_storage_region(
                HG38_PATH,
                expected_regions=("IMAGINARY REGION"),
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
