"""This module contains Hail Batch-specific extensions of the generic _Pipeline and _Step classes"""

from __future__ import annotations

import os
import stat
import tempfile
from enum import Enum
from typing import Union

import hailtop.batch as hb

from .pipeline import _Pipeline, _Step, LocalizationStrategy, DelocalizationStrategy
from .utils import check_gcloud_storage_region

DEFAULT_PYTHON_IMAGE = "hailgenetics/hail:0.2.67"


class BatchStepType(Enum):
    PYTHON = "python"
    BASH = "bash"


class _BatchPipeline(_Pipeline):
    """This class contains Hail Batch-specific extensions of the _Pipeline class"""

    def __init__(self, argument_parser, name=None):
        """
        _BatchPipeline constructor

        :param argument_parser: A configargparse.ArgumentParser object
        :param name: Pipeline name
        """
        super().__init__(argument_parser, name=name)

        self._argument_parser = argument_parser
        batch_args = argument_parser.add_argument_group("hail batch")
        batch_args.add_argument(
            "--batch-billing-project",
            env_var="BATCH_BILLING_PROJECT",
            required=True,
            help="Batch requires a billing project for compute costs. To set up a billing project, email the hail team.")
        batch_args.add_argument(
            "--batch-temp-bucket",
            env_var="BATCH_TEMP_BUCKET",
            required=True,
            help="Batch requires a temp bucket it can use to store files. The Batch service account must have Admin access "
                 "to this bucket. To get the name of your Batch service account, go to https://auth.hail.is/user. Then, to "
                 "grant Admin permissions, run "
                 "gsutil iam ch serviceAccount:[SERVICE_ACCOUNT_NAME]:objectAdmin gs://[BUCKET_NAME]")
        batch_args.add_argument(
            "--batch-cancel-after-n-failures",
            type=int,
            help="Cancel batch if this number of jobs fail."
        )
        batch_args.add_argument(
            "--batch-default-image",
            help="Default docker container."
        )
        batch_args.add_argument(
            "--batch-default-python-image",
            help="Default docker container to use for Python jobs.",
            default=DEFAULT_PYTHON_IMAGE,
        )
        batch_args.add_argument(
            "--batch-open-ui",
            action="store_true",
            help="Open batch dashboard page after submitting the jobs."
        )
        batch_args.add_argument(
            "--batch-wait",
            action="store_true",
            help="Don't exit until the batch completes."
        )
        batch_args.add_argument(
            "--batch-disable-progress-bar",
            action="store_true",
            help="Disable progress bar."
        )

        gcloud_args = argument_parser.add_argument_group("google cloud")
        gcloud_args.add_argument(
            "--gcloud-project",
            env_var="GCLOUD_PROJECT",
            help="If specified, this project will be passed as an argument in optional operations such as gsutil cp, "
                 "gcloud auth, etc.")

        gcloud_args.add_argument(
            "--gcloud-credentials-path",
            help="Google bucket path of gcloud credentials. This is required if you use switch_gcloud_auth_to_user_account(..)",
        )
        gcloud_args.add_argument(
            "--gcloud-user-account",
            help="Google user account. This is required if you use switch_gcloud_auth_to_user_account(..)",
        )
        gcloud_args.add_argument(
            "--acceptable-storage-regions",
            nargs="*",
            default=("US", "US-CENTRAL1"),
        )

        self._requester_pays_project = None
        self._cancel_after_n_failures = None
        self._default_image = None
        self._default_python_image = None
        self._default_memory = None
        self._default_cpu = None
        self._default_storage = None
        self._default_timeout = None

    def new_step(
            self,
            short_name: str,
            step_number: int = None,
            depends_on: _Step = None,
            image: str = None,
            cpu: Union[str, float, int] = None,
            memory: Union[str, float, int] = None,
            storage: Union[str, int] = None,
            always_run: bool = False,
            timeout: Union[float, int] = None,
            write_commands_to_script: bool = False,
            save_script_to_output_dir: bool = False,
            profile_cpu_and_memory_usage: bool = False,
            reuse_job_from_previous_step: bool = None,
    ):
        """

        :param short_name:
        :param step_number:
        :param depends_on:
        :param image:
        :param cpu:
        :param memory:
        :param storage:
        :param always_run:
        :param timeout:
        :param reuse_job_from_previous_step: _Step object from which to reuse job
        :return: new _Step
        """

        batch_step = _BatchStep(
            self,
            short_name,
            step_number=step_number,
            image=image,
            cpu=cpu,
            memory=memory,
            storage=storage,
            always_run=always_run,
            timeout=timeout,
            write_commands_to_script=write_commands_to_script,
            save_script_to_output_dir=save_script_to_output_dir,
            profile_cpu_and_memory_usage=profile_cpu_and_memory_usage,
            reuse_job_from_previous_step=reuse_job_from_previous_step,
        )

        if depends_on:
            batch_step.depends_on(depends_on)

        # register the _Step
        self._all_steps.append(batch_step)

        return batch_step

    def requester_pays_project(self, requester_pays_project):
        """
        :param requester_pays_project: The name of the Google project to be billed when accessing requester pays
            buckets.
        """
        self._requester_pays_project = requester_pays_project
        return self

    def cancel_after_n_failures(self, cancel_after_n_failures):
        """
        :param cancel_after_n_failures: Automatically cancel the batch after N failures have occurred.
        """
        self._cancel_after_n_failures = cancel_after_n_failures
        return self

    def default_image(self, default_image):
        """
        :param default_image:  (Optional[str]) – Default docker image to use for Bash jobs. This must be the full name
            of the image including any repository prefix and tags if desired (default tag is latest).
        """
        self._default_image = default_image
        return self

    def default_python_image(self, default_python_image):
        """
        :param default_python_image:  (Optional[str]) – The image to use for Python jobs.
            The image specified must have the dill package installed. If default_python_image is not specified,
            then a Docker image will automatically be created for you with the base image
            hailgenetics/python-dill:[major_version].[minor_version]-slim and the Python packages specified by
            python_requirements will be installed. The default name of the image is batch-python with a random string
            for the tag unless python_build_image_name is specified. If the ServiceBackend is the backend, the locally
            built image will be pushed to the repository specified by image_repository.
        """
        self._default_python_image = default_python_image
        return self

    def default_memory(self, default_memory: Union[str, int]):
        """
        :param default_memory: (Union[int, str, None]) – Memory setting to use by default if not specified by a job.
            Only applicable if a docker image is specified for the LocalBackend or the ServiceBackend. See Job.memory().
        """
        self._default_memory = default_memory
        return self

    def default_cpu(self, default_cpu: Union[str, int, float]):
        """
        :param default_cpu: (Union[float, int, str, None]) – CPU setting to use by default if not specified by a job.
            Only applicable if a docker image is specified for the LocalBackend or the ServiceBackend. See Job.cpu().
        """
        self._default_cpu = default_cpu
        return self

    def default_storage(self, default_storage: Union[str, int]):
        """
        :param default_storage: Storage setting to use by default if not specified by a job. Only applicable for the
            ServiceBackend. See Job.storage().
        """
        self._default_storage = default_storage
        return self

    def default_timeout(self, default_timeout):
        """
        :param default_timeout: Maximum time in seconds for a job to run before being killed. Only applicable for the
            ServiceBackend. If None, there is no timeout.
        """
        self._default_timeout = default_timeout
        return self

    def run(self):
        """
        Pass pipeline to the Batch service
        """
        args = self._get_args()

        try:
            self._create_batch_obj()

            num_steps_transferred = self._transfer_all_steps()

            if num_steps_transferred == 0:
                print("No steps to run. Exiting..")
                return

            self._run_batch_obj()
        finally:
            if isinstance(self._backend, hb.ServiceBackend):
                self._backend.close()

    def _create_batch_obj(self):
        args = self._get_args()

        self._backend = hb.ServiceBackend(billing_project=args.batch_billing_project, bucket=args.batch_temp_bucket)
        self._batch = hb.Batch(
            backend=self._backend,
            name=self.name,
            project=args.gcloud_project,
            requester_pays_project=args.gcloud_project,  # The name of the Google project to be billed when accessing requester pays buckets.
            cancel_after_n_failures=self._cancel_after_n_failures or args.batch_cancel_after_n_failures,  # Automatically cancel the batch after N failures have occurre
            default_image=self._default_image or args.batch_default_image,  #(Optional[str]) – Default docker image to use for Bash jobs. This must be the full name of the image including any repository prefix and tags if desired (default tag is latest).
            default_python_image=self._default_python_image or args.default_python_image,
            default_memory=self._default_memory, # (Union[int, str, None]) – Memory setting to use by default if not specified by a job. Only applicable if a docker image is specified for the LocalBackend or the ServiceBackend. See Job.memory().
            default_cpu=self._default_cpu,  # (Union[float, int, str, None]) – CPU setting to use by default if not specified by a job. Only applicable if a docker image is specified for the LocalBackend or the ServiceBackend. See Job.cpu().
            default_storage=self._default_storage,  # Storage setting to use by default if not specified by a job. Only applicable for the ServiceBackend. See Job.storage().
            default_timeout=self._default_timeout,  # Maximum time in seconds for a job to run before being killed. Only applicable for the ServiceBackend. If None, there is no timeout.
        )

    def _run_batch_obj(self):
        args = self._get_args()

        self._batch.run(
            dry_run=args.dry_run,
            verbose=args.verbose,
            delete_scratch_on_exit=None,  # If True, delete temporary directories with intermediate files
            wait=args.batch_wait,  # If True, wait for the batch to finish executing before returning
            open=args.batch_open_ui,  # If True, open the UI page for the batch
            disable_progress_bar=args.batch_disable_progress_bar,  # If True, disable the progress bar.
            callback=None,  # If not None, a URL that will receive at most one POST request after the entire batch completes.
        )


class _BatchStep(_Step):
    """This class contains Hail Batch-specific extensions of the _Step class"""

    def __init__(
            self,
            pipeline,
            short_name,
            arg_name=None,
            step_number=None,
            image=None,
            cpu=None,
            memory=None,
            storage=None,
            always_run=False,
            timeout=None,
            output_dir=None,
            step_type=BatchStepType.BASH,
            default_localization_strategy=None,
            default_delocalization_strategy=None,
            write_commands_to_script=False,
            save_script_to_output_dir=False,
            profile_cpu_and_memory_usage=False,
            reuse_job_from_previous_step=None,
    ):
        super().__init__(
            pipeline,
            short_name,
            arg_name=arg_name,
            step_number=step_number,
            output_dir=output_dir,
            default_localization_strategy=default_localization_strategy,
            default_delocalization_strategy=default_delocalization_strategy,
        )

        self._image = image
        self._cpu = cpu
        self._memory = memory
        self._storage = storage
        self._always_run = always_run
        self._timeout = timeout
        self._step_type = step_type
        self._write_commands_to_script = write_commands_to_script
        self._save_script_to_output_dir = save_script_to_output_dir
        self._profile_cpu_and_memory_usage = profile_cpu_and_memory_usage
        self._reuse_job_from_previous_step = reuse_job_from_previous_step

        self._job = None
        self._output_file_counter = 0

        self._paths_localized_via_temp_bucket = set()
        self._buckets_mounted_via_gcsfuse = set()

    def cpu(self, cpu: Union[str, int]) -> _Step:
        self._cpu = cpu
        return self

    def memory(self, memory: Union[str, int, float]) -> _Step:
        self._memory = memory
        return self

    def storage(self, storage: Union[str, int]) -> _Step:
        self._storage = storage
        return self

    def always_run(self, always_run: bool) -> _Step:
        self._always_run = always_run
        return self

    def timeout(self, timeout: Union[float, int]) -> _Step:
        self._timeout = timeout
        return self

    def _transfer_step(self):
        """This method is called if the step does need to run"""

        # make Job object
        batch = self._pipeline._batch
        if self._reuse_job_from_previous_step:
            # reuse previous Job
            if self._reuse_job_from_previous_step._job:
                self._job = self._reuse_job_from_previous_step._job
            else:
                raise Exception(f"previous job not set: {self._reuse_job_from_previous_step}")
        else:
            # create new job
            if self._step_type == BatchStepType.PYTHON:
                self._job = batch.new_python_job(name=self.short_name)
            elif self._step_type == BatchStepType.BASH:
                self._job = batch.new_bash_job(name=self.short_name)
            else:
                raise ValueError(f"Unexpected BatchStepType: {self._step_type}")

        self._unique_batch_id = abs(hash(batch)) % 10**9
        self._unique_job_id = abs(hash(self._job)) % 10**9

        # set execution parameters
        if self._image:
            self._job.image(self._image)

        if self._cpu is not None:
            if self._cpu < 0.25 or self._cpu > 16:
                raise ValueError(f"CPU arg is {self._cpu}. This is outside the range of 0.25 to 16 CPUs")

            self._job.cpu(self._cpu)  # Batch default is 1

        if self._memory is not None:
            if isinstance(self._memory, int) or isinstance(self._memory, float):
                if self._memory < 0.1 or self._memory > 60:
                    raise ValueError(f"Memory arg is {self._memory}. This is outside the range of 0.1 to 60 Gb")

                self._job.memory(f"{self._memory}Gi")  # Batch default is 3.75G
            elif isinstance(self._memory, str):
                self._job.memory(self._memory)
            else:
                raise ValueError(f"Unexpected memory arg type: {type(self._memory)}")

        if self._storage is not None:
            self._job.storage(self._storage)

        if self._timeout is not None:
            self._job.timeout(self._timeout)

        if self._always_run is not None:
            self._job.always_run(self._always_run)

        # transfer job dependencies
        for upstream_step in self._upstream_steps:
            if upstream_step._job:
                self._job.depends_on(upstream_step._job)

        # transfer inputs
        for input_spec in self._inputs:
            self._transfer_input(input_spec)

        # transfer commands
        if self._write_commands_to_script:
            # write to script
            args = self._get_args()

            script_lines = []
            # set bash options for easier debugging and to make command execution more robust
            script_lines.append("set -euxo pipefail")
            for command in self._commands:
                script_lines.append(command)

            script_file = tempfile.NamedTemporaryFile("wt", prefix="script_", suffix=".sh", encoding="UTF-8", delete=True)
            script_file.writelines(script_lines)
            script_file.flush()

            # upload script to the temp bucket
            script_temp_gcloud_path = os.path.join(
                f"gs://{args.batch_temp_bucket}/batch_{self._unique_batch_id}/job_{self._unique_job_id}",
                os.path.basename(script_file.name))

            os.chmod(script_file.name, mode=stat.S_IREAD | stat.S_IEXEC)
            script_file_upload_command = self._generate_gsutil_copy_command(script_file.name, script_temp_gcloud_path)
            os.system(script_file_upload_command)
            script_file.close()

            script_input_spec = self.input(script_temp_gcloud_path)
            self._transfer_input(script_input_spec)
            self._job.command(f"bash -c '{script_input_spec['local_path']}'")
        else:
            for command in self._commands:
                print(f"Adding command: {command}")
                self._job.command(command)

        # transfer outputs
        for output_spec in self._outputs:
            self._transfer_output(output_spec)

        # clean up any files that were copied to the temp bucket
        cleanup_job_name = f"{self.short_name} cleanup {len(self._paths_localized_via_temp_bucket)} files"
        cleanup_job = self._pipeline._batch.new_job(name=cleanup_job_name)
        cleanup_job.depends_on(self._job)
        cleanup_job.always_run()
        for temp_file_path in self._paths_localized_via_temp_bucket:
            cleanup_job.command(f"gsutil -m rm -r {temp_file_path}")
        self._paths_localized_via_temp_bucket = set()

    def _get_supported_localization_strategies(self):
        return super()._get_supported_localization_strategies() | {
            LocalizationStrategy.COPY,
            LocalizationStrategy.GSUTIL_COPY,
            LocalizationStrategy.HAIL_BATCH_GCSFUSE,
            LocalizationStrategy.HAIL_BATCH_GCSFUSE_VIA_TEMP_BUCKET,
        }

    def _get_supported_delocalization_strategies(self):
        return super()._get_supported_delocalization_strategies() | {
            DelocalizationStrategy.COPY,
            DelocalizationStrategy.GSUTIL_COPY,
        }

    def _preprocess_input(self, input_spec):
        source_path = input_spec["source_path"]
        local_path = input_spec["local_path"]
        local_dir = input_spec["local_dir"]

        localization_strategy = input_spec["localization_strategy"]
        if localization_strategy in super()._get_supported_localization_strategies():
            super()._preprocess_input(input_spec)
        elif localization_strategy == LocalizationStrategy.GSUTIL_COPY:
            if not source_path.startswith("gs://"):
                raise ValueError(f"Expected gs:// path but instead found '{local_dir}'")
            self.command(f"mkdir -p '{local_dir}'")
            self.command(self._generate_gsutil_copy_command(source_path, local_dir))
            self.command(f"ls -lh '{local_path}'")   # check that file was copied successfully
        elif localization_strategy in (
                LocalizationStrategy.COPY,
                LocalizationStrategy.HAIL_BATCH_GCSFUSE,
                LocalizationStrategy.HAIL_BATCH_GCSFUSE_VIA_TEMP_BUCKET):
            pass  # these will be handled in _transfer_input(..)
        else:
            raise ValueError(f"Unsupported localization strategy: {localization_strategy}")

    def _transfer_input(self, input_spec):
        args = self._get_args()
        if args.acceptable_storage_regions:
            check_gcloud_storage_region(
                input_spec,
                expected_regions=args.acceptable_storage_regions,
                gcloud_project=args.gcloud_project,
                verbose=args.verbose)

        localization_strategy = input_spec["localization_strategy"]
        source_path = input_spec["source_path"]
        local_path = input_spec["local_path"]
        local_dir = input_spec["local_dir"]

        if localization_strategy in super()._get_supported_localization_strategies():
            super()._preprocess_input(input_spec)
        elif localization_strategy == LocalizationStrategy.GSUTIL_COPY:
            pass  # GSUTIL_COPY was already handled in _preprocess_input(..)
        elif localization_strategy == LocalizationStrategy.COPY:
            input_obj = self._job._batch.read_input(source_path)
            self._job.command(f"mkdir -p '{local_dir}'")
            self._job.command(f"mv {input_obj} {local_path}")
        elif localization_strategy in (
                LocalizationStrategy.HAIL_BATCH_GCSFUSE,
                LocalizationStrategy.HAIL_BATCH_GCSFUSE_VIA_TEMP_BUCKET):
            self._handle_input_transfer_using_gcsfuse(input_spec)
        else:
            raise ValueError(f"Unsupported localization strategy: {localization_strategy}")

    def _generate_gsutil_copy_command(self, source_path, destination_dir):
        args = self._get_args()
        gsutil_command = f"gsutil"
        if args.gcloud_project:
            gsutil_command += f" -u {args.gcloud_project}"
        return f"time {gsutil_command} -m cp -r '{source_path}' '{destination_dir}'"

    def _handle_input_transfer_using_gcsfuse(self, input_spec):
        args = self._get_args()

        source_path = input_spec["source_path"]
        source_path_without_protocol = input_spec["source_path_without_protocol"]
        #filename = input_spec["filename"]

        localization_strategy = input_spec["localization_strategy"]
        if localization_strategy == LocalizationStrategy.HAIL_BATCH_GCSFUSE_VIA_TEMP_BUCKET:
            if not args.batch_temp_bucket:
                raise ValueError("--temp-bucket not specified.")

            source_bucket = args.batch_temp_bucket
            temp_dir = os.path.join(f"gs://{source_bucket}/batch_{self._unique_batch_id}/job_{self._unique_job_id}", source_path_without_protocol.strip("/")) + "/"
            temp_file_path = os.path.join(temp_dir, input_spec["filename"])

            if temp_file_path in self._paths_localized_via_temp_bucket:
                raise ValueError(f"{source_path} has already been localized via temp bucket.")
            self._paths_localized_via_temp_bucket.add(temp_file_path)

            # copy file to temp bucket
            self._job.command(self._generate_gsutil_copy_command(source_path, temp_dir))
        else:
            subdir = localization_strategy.get_subdir_name()
            source_bucket = input_spec["source_bucket"]

        local_mount_dir = os.path.join(input_spec["destination_root_dir"], subdir, source_bucket)
        if source_bucket not in self._buckets_mounted_via_gcsfuse:
            self._job.command(f"mkdir -p {local_mount_dir}")
            self._job.gcsfuse(source_bucket, local_mount_dir, read_only=True)
            self._buckets_mounted_via_gcsfuse.add(source_bucket)

    def _preprocess_output(self, output_spec):
        local_path = output_spec["local_path"]
        destination_dir = output_spec["destination_dir"]

        delocalization_strategy = output_spec["delocalization_strategy"]
        if delocalization_strategy in super()._get_supported_delocalization_strategies():
            super()._preprocess_output(output_spec)
        elif delocalization_strategy == DelocalizationStrategy.COPY:
            pass
        elif delocalization_strategy == DelocalizationStrategy.GSUTIL_COPY:
            self._add_commands_for_gsutil_copy(local_path, destination_dir)

    def _transfer_output(self, output_spec):
        local_path = output_spec["local_path"]
        destination_dir = output_spec["destination_dir"]
        destination_filename = output_spec["destination_filename"]

        delocalization_strategy = output_spec["delocalization_strategy"]
        if delocalization_strategy in super()._get_supported_delocalization_strategies():
            super()._preprocess_output(output_spec)
        elif delocalization_strategy == DelocalizationStrategy.COPY:
            self._output_file_counter += 1
            output_file_obj = self._job[f"ofile{self._output_file_counter}"]
            self._job.command(f'cp {local_path} {output_file_obj}')
            self._job._batch.write_output(output_file_obj, os.path.join(destination_dir, destination_filename))
        elif delocalization_strategy == DelocalizationStrategy.GSUTIL_COPY:
            pass  # GSUTIL_COPY was already handled in _preprocess_output(..)
        else:
            raise ValueError(f"Unsupported delocalization strategy: {delocalization_strategy}")
