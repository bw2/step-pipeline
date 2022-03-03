"""This module contains Hail Batch-specific extensions of the _Pipeline and _Step classes"""

import os
import stat
import tempfile
from enum import Enum

try:
    import hailtop.batch as hb
except ValueError as e:
    print(f"WARNING: unable to import hailtop.batch: {e}")

from .constants import Backend
from .pipeline import _Pipeline, _Step, Localize, Delocalize
from .utils import check_gcloud_storage_region

# TODO get latest tag via https://hub.docker.com/v2/repositories/hailgenetics/genetics/tags/ ?
DEFAULT_BASH_IMAGE = DEFAULT_PYTHON_IMAGE = "hailgenetics/hail:0.2.77"


class _BatchStepType(Enum):
    PYTHON = "python"
    BASH = "bash"


class _BatchPipeline(_Pipeline):
    """This class contains Hail Batch-specific extensions of the _Pipeline class"""

    def __init__(self, name=None, config_arg_parser=None, backend=Backend.HAIL_BATCH_SERVICE):
        """
        _BatchPipeline constructor

        Args:
            name (str): Pipeline name
            config_arg_parser (configargparse): The configargparse.ArgumentParser object to use for defining
                command-line args
            backend (Backend): Either Backend.HAIL_BATCH_SERVICE or Backend.HAIL_BATCH_LOCAL
        """
        super().__init__(name=name, config_arg_parser=config_arg_parser)

        self._argument_parser = config_arg_parser
        batch_args = config_arg_parser.add_argument_group("hail batch")

        grp = batch_args.add_mutually_exclusive_group()
        grp.add_argument("--local", action="store_true", help="Run the pipeline locally using Batch LocalBackend")
        grp.add_argument("--cluster", action="store_true", help="Run the pipeline on the Batch cluster")

        batch_args.add_argument(
            "--batch-billing-project",
            env_var="BATCH_BILLING_PROJECT",
            help="Batch requires a billing project to charge for compute costs. To set up a billing project, email the "
                 "hail team."
        )

        batch_args.add_argument(
            "--batch-remote-tmpdir",
            env_var="BATCH_REMOTE_TMPDIR",
            help="Batch requires a temp cloud storage path that it can use to store intermediate files. The Batch "
                 "service account must have Admin access to this directory. To get the name of your Batch "
                 "service account, go to https://auth.hail.is/user. Then, to grant Admin permissions, run "
                 "gsutil iam ch serviceAccount:[SERVICE_ACCOUNT_NAME]:objectAdmin gs://[BUCKET_NAME]"
        )

        gcloud_args = config_arg_parser.add_argument_group("google cloud")
        gcloud_args.add_argument(
            "--gcloud-project",
            env_var="GCLOUD_PROJECT",
            help="The Google Cloud project to use for accessing requester-pays buckets, etc."
        )
        gcloud_args.add_argument(
            "--gcloud-credentials-path",
            help="Google bucket path of gcloud credentials to use in step.switch_gcloud_auth_to_user_account(..)."
                 "See the docs of that method for details.",
        )
        gcloud_args.add_argument(
            "--gcloud-user-account",
            help="Google user account to use in step.switch_gcloud_auth_to_user_account(..). See the docs of that "
                 "method for details.",
        )
        gcloud_args.add_argument(
            "--acceptable-storage-regions",
            nargs="*",
            default=("US", "US-CENTRAL1"),
            help="If specified, the pipeline will confirm that input buckets are in one of these regions "
                 "to avoid egress charges",
        )

        args = self.parse_args()

        # if --local or --cluster were specified, override the service
        if args.local:
            backend = Backend.HAIL_BATCH_LOCAL
        elif args.cluster:
            backend = Backend.HAIL_BATCH_SERVICE

        self._backend_type = backend
        self._requester_pays_project = args.gcloud_project
        self._cancel_after_n_failures = None
        self._default_image = DEFAULT_BASH_IMAGE
        self._default_python_image = DEFAULT_PYTHON_IMAGE
        self._default_memory = None
        self._default_cpu = None
        self._default_storage = None
        self._default_timeout = None
        self._default_output_dir = None
        self._backend = None

    def get_backend_type(self):
        """Returns either Backend.HAIL_BATCH_SERVICE or Backend.HAIL_BATCH_LOCAL"""
        return self._backend_type

    def new_step(
        self,
        short_name=None,
        step_number=None,
        arg_suffix=None,
        depends_on=None,
        image=None,
        cpu=None,
        memory=None,
        storage=None,
        always_run=False,
        timeout=None,
        output_dir=None,
        reuse_job_from_previous_step=None,
        localize_by=Localize.COPY,
        delocalize_by=Delocalize.COPY,
    ):
        """
        Args:
            short_name (str): A short name for this Step.
            step_number (int): optional Step number which serves another alias for this step in addition to short_name.
            arg_suffix (str): optional suffix for the command-line args that will be created for forcing or skipping
                execution of this Step.
            depends_on (_Step): Optional upstream Step that this Step depends on.
            image (str): Docker image to use for this Step.
            cpu (str, float, int): CPU requirements. Units are in cpu if cores is numeric.
            memory (str, float int): Memory requirements. The memory expression must be of the form {number}{suffix}
                where valid optional suffixes are K, Ki, M, Mi, G, Gi, T, Ti, P, and Pi. Omitting a suffix means
                the value is in bytes. For the ServiceBackend, the values ‘lowmem’, ‘standard’, and ‘highmem’ are also
                valid arguments. ‘lowmem’ corresponds to approximately 1 Gi/core, ‘standard’ corresponds to
                approximately 4 Gi/core, and ‘highmem’ corresponds to approximately 7 Gi/core. The default value
                is ‘standard’.
            storage (str, int): Disk size. The storage expression must be of the form {number}{suffix} where valid
                optional suffixes are K, Ki, M, Mi, G, Gi, T, Ti, P, and Pi. Omitting a suffix means the value is in
                bytes. For the ServiceBackend, jobs requesting one or more cores receive 5 GiB of storage for the root
                file system /. Jobs requesting a fraction of a core receive the same fraction of 5 GiB of storage.
                If you need additional storage, you can explicitly request more storage using this method and the extra
                storage space will be mounted at /io. Batch automatically writes all ResourceFile to /io.
                The default storage size is 0 Gi. The minimum storage size is 0 Gi and the maximum storage size is
                64 Ti. If storage is set to a value between 0 Gi and 10 Gi, the storage request is rounded up to 10 Gi.
                All values are rounded up to the nearest Gi.
            always_run (bool): Set the Step to always run, even if dependencies fail.
            timeout (float, int): Set the maximum amount of time this job can run for before being killed.
            output_dir (str): Optional default output directory for Step outputs.
            reuse_job_from_previous_step (_Step): Optionally, reuse the batch.Job object from this other upstream _Step.
            localize_by (Localize): If specified, this will be the default Localize approach used by Step inputs.
            delocalize_by (Delocalize): If specified, this will be the default Delocalize approach used by Step outputs.
        Return:
            _BatchStep: The new _BatchStep object.
        """

        batch_step = _BatchStep(
            self,
            short_name=short_name,
            step_number=step_number,
            arg_suffix=arg_suffix,
            image=image,
            cpu=cpu,
            memory=memory,
            storage=storage,
            always_run=always_run,
            timeout=timeout,
            output_dir=self._default_output_dir or output_dir,
            reuse_job_from_previous_step=reuse_job_from_previous_step,
            localize_by=localize_by,
            delocalize_by=delocalize_by,
        )

        if depends_on:
            batch_step.depends_on(depends_on)

        # register the _Step
        self._all_steps.append(batch_step)

        return batch_step

    def requester_pays_project(self, requester_pays_project):
        """Set the requester_pays_project value.

        Args:
            requester_pays_project (str): The name of the Google Cloud project to be billed when accessing
                requester-pays buckets.
        """
        self._requester_pays_project = requester_pays_project
        return self

    def cancel_after_n_failures(self, cancel_after_n_failures):
        """Set the cancel_after_n_failures value.

            Args:
                cancel_after_n_failures: (int): Automatically cancel the batch after N failures have occurred.
        """
        self._cancel_after_n_failures = cancel_after_n_failures
        return self

    def default_image(self, default_image):
        """Set the default Docker image to use for Steps in this pipeline.

        Args:
            default_image (str): Default docker image to use for Bash jobs. This must be the full name
            of the image including any repository prefix and tags if desired (default tag is latest).
        """
        self._default_image = default_image
        return self

    def default_python_image(self, default_python_image):
        """Set the default image for Python Jobs.

        Args:
            default_python_image (str): The Docker image to use for Python jobs. The image specified must have the dill
                package installed. If default_python_image is not specified, then a Docker image will automatically be
                created for you with the base image hailgenetics/python-dill:[major_version].[minor_version]-slim and
                the Python packages specified by python_requirements will be installed. The default name of the image
                is batch-python with a random string for the tag unless python_build_image_name is specified. If the
                ServiceBackend is the backend, the locally built image will be pushed to the repository specified by
                image_repository.
        """
        self._default_python_image = default_python_image
        return self

    def default_memory(self, default_memory):
        """Set the default memory usage.

        Args:
            default_memory (int, str): Memory setting to use by default if not specified by a Step.
                Only applicable if a docker image is specified for the LocalBackend or the ServiceBackend.
                See Job.memory().
        """
        self._default_memory = default_memory
        return self

    def default_cpu(self, default_cpu):
        """Set the default cpu requirement.

        Args:
            default_cpu (float, int, str): CPU setting to use by default if not specified by a job.
                Only applicable if a docker image is specified for the LocalBackend or the ServiceBackend.
                See Job.cpu().
        """
        self._default_cpu = default_cpu
        return self

    def default_storage(self, default_storage):
        """Set the default storage disk size.

        Args:
            default_storage (str, int): Storage setting to use by default if not specified by a job. Only applicable
                for the ServiceBackend. See Job.storage().
        """
        self._default_storage = default_storage
        return self

    def default_timeout(self, default_timeout):
        """Set the default job timeout duration.

        Args:
            default_timeout: Maximum time in seconds for a job to run before being killed. Only applicable for the
                ServiceBackend. If None, there is no timeout.
        """
        self._default_timeout = default_timeout
        return self

    def default_output_dir(self, default_output_dir):
        """Set the default output_dir for pipeline Steps.

        Args:
            output_dir (str): Output directory
        """
        self._default_output_dir = default_output_dir
        return self

    def run(self):
        """Batch-specific code for submitting the pipeline to the Hail Batch backend"""

        print(f"Starting {self.name or ''} pipeline:")
        # confirm that all required command-line args were specified
        self._argument_parser.parse_args()

        try:
            self._create_batch_obj()

            num_steps_transferred = self._transfer_all_steps()

            if num_steps_transferred == 0:
                print("No steps to run. Exiting..")
                return

            result = self._run_batch_obj()
            return result
        finally:
            if isinstance(self._backend, hb.ServiceBackend):
                self._backend.close()

    def _get_localization_root_dir(self, localize_by):
        """Return the top-level root directory where localized files will be copied"""
        return "/io"

    def _create_batch_obj(self):
        """Instanciate the Hail Batch Backend."""

        args = self.parse_args()

        if self._backend_type == Backend.HAIL_BATCH_LOCAL:
            self._backend = hb.LocalBackend()
        elif self._backend_type == Backend.HAIL_BATCH_SERVICE:
            if not args.batch_billing_project:
                raise ValueError("--batch-billing-project must be set when --cluster is used")
            if not args.batch_remote_tmpdir:
                raise ValueError("--batch-remote-tmpdir must be set when --cluster is used")
            self._backend = hb.ServiceBackend(
                google_project=args.gcloud_project,
                billing_project=args.batch_billing_project,
                remote_tmpdir=args.batch_remote_tmpdir)
        else:
            raise Exception(f"Unexpected _backend_type: {self._backend_type}")

        self._batch = hb.Batch(
            backend=self._backend,
            name=self.name,
            requester_pays_project=self._requester_pays_project,  # The name of the Google project to be billed when accessing requester pays buckets.
            cancel_after_n_failures=self._cancel_after_n_failures,  # Automatically cancel the batch after N failures have occurre
            default_image=self._default_image,  #(Optional[str]) – Default docker image to use for Bash jobs. This must be the full name of the image including any repository prefix and tags if desired (default tag is latest).
            default_python_image=self._default_python_image,
            default_memory=self._default_memory, # (Union[int, str, None]) – Memory setting to use by default if not specified by a job. Only applicable if a docker image is specified for the LocalBackend or the ServiceBackend. See Job.memory().
            default_cpu=self._default_cpu,  # (Union[float, int, str, None]) – CPU setting to use by default if not specified by a job. Only applicable if a docker image is specified for the LocalBackend or the ServiceBackend. See Job.cpu().
            default_storage=self._default_storage,  # Storage setting to use by default if not specified by a job. Only applicable for the ServiceBackend. See Job.storage().
            default_timeout=self._default_timeout,  # Maximum time in seconds for a job to run before being killed. Only applicable for the ServiceBackend. If None, there is no timeout.
        )

    def _run_batch_obj(self):
        """Launch the Hail Batch pipeline and return the result."""

        args = self.parse_args()

        if self._backend_type == Backend.HAIL_BATCH_LOCAL:
            # Hail Batch LocalBackend mode doesn't support some of the args suported by ServiceBackend
            result = self._batch.run(dry_run=args.dry_run, verbose=args.verbose)
        elif self._backend_type == Backend.HAIL_BATCH_SERVICE:
            result = self._batch.run(
                dry_run=args.dry_run,
                verbose=args.verbose,
                delete_scratch_on_exit=None,  # If True, delete temporary directories with intermediate files
                wait=True,  # If True, wait for the batch to finish executing before returning
                open=False,  # If True, open the UI page for the batch
                disable_progress_bar=False,  # If True, disable the progress bar.
                callback=None,  # If not None, a URL that will receive at most one POST request after the entire batch completes.
            )
        else:
            raise Exception(f"Unexpected _backend_type: {self._backend_type}")

        # The Batch pipeline returns an undocumented result object which can be used to retrieve the Job's status code
        # and logs
        return result


class _BatchStep(_Step):
    """This class contains Hail Batch-specific extensions of the _Step class"""

    def __init__(
        self,
        pipeline,
        short_name=None,
        step_number=None,
        arg_suffix=None,
        image=None,
        cpu=None,
        memory=None,
        storage=None,
        always_run=False,
        timeout=None,
        output_dir=None,
        reuse_job_from_previous_step=None,
        localize_by=Localize.COPY,
        delocalize_by=Delocalize.COPY,
    ):
        """Step constructor.

        Args:
            pipeline (_BatchPipeline): The pipeline that this Step is a part of.
            short_name (str): Step name
            step_number (int): optional Step number which serves another alias for this step in addition to short_name.
            arg_suffix (str): optional suffix for the command-line args that will be created for forcing or skipping
                execution of this Step.
            image (str): Docker image to use for this Step
            cpu (str, float, int): CPU requirements. Units are in cpu if cores is numeric.
            memory (str, float int): Memory requirements. The memory expression must be of the form {number}{suffix}
                where valid optional suffixes are K, Ki, M, Mi, G, Gi, T, Ti, P, and Pi. Omitting a suffix means
                the value is in bytes. For the ServiceBackend, the values ‘lowmem’, ‘standard’, and ‘highmem’ are also
                valid arguments. ‘lowmem’ corresponds to approximately 1 Gi/core, ‘standard’ corresponds to
                approximately 4 Gi/core, and ‘highmem’ corresponds to approximately 7 Gi/core. The default value
                is ‘standard’.
            storage (str, int): Disk size. The storage expression must be of the form {number}{suffix} where valid
                optional suffixes are K, Ki, M, Mi, G, Gi, T, Ti, P, and Pi. Omitting a suffix means the value is in
                bytes. For the ServiceBackend, jobs requesting one or more cores receive 5 GiB of storage for the root
                file system /. Jobs requesting a fraction of a core receive the same fraction of 5 GiB of storage.
                If you need additional storage, you can explicitly request more storage using this method and the extra
                storage space will be mounted at /io. Batch automatically writes all ResourceFile to /io.
                The default storage size is 0 Gi. The minimum storage size is 0 Gi and the maximum storage size is
                64 Ti. If storage is set to a value between 0 Gi and 10 Gi, the storage request is rounded up to 10 Gi.
                All values are rounded up to the nearest Gi.
            always_run (bool): Set the Step to always run, even if dependencies fail.
            timeout (float, int): Set the maximum amount of time this job can run for before being killed.
            output_dir (str): Optional default output directory for Step outputs.
            reuse_job_from_previous_step (_Step): Optionally, reuse the batch.Job object from this other upstream _Step.
            localize_by (Localize): If specified, this will be the default Localize approach used by Step inputs.
            delocalize_by (Delocalize): If specified, this will be the default Delocalize approach used by Step outputs.
        """
        super().__init__(
            pipeline,
            short_name,
            step_number=step_number,
            arg_suffix=arg_suffix,
            output_dir=output_dir,
            localize_by=localize_by,
            delocalize_by=delocalize_by,
        )

        self._image = image
        self._cpu = cpu
        self._memory = memory
        self._storage = storage
        self._always_run = always_run
        self._timeout = timeout
        self._reuse_job_from_previous_step = reuse_job_from_previous_step

        self._job = None
        self._output_file_counter = 0

        self._paths_localized_via_temp_bucket = set()
        self._buckets_mounted_via_gcsfuse = set()

        self._step_type = _BatchStepType.BASH
        self._write_commands_to_script = False

    def cpu(self, cpu):
        """Set the CPU requirement for this Step.
        Args:
            cpu (str, float, int): CPU requirements. Units are in cpu if cores is numeric.
        """
        self._cpu = cpu
        return self

    def memory(self, memory):
        """Set the memory requirement for this Step.
        Args:
            memory (str, float int): Memory requirements. The memory expression must be of the form {number}{suffix}
                where valid optional suffixes are K, Ki, M, Mi, G, Gi, T, Ti, P, and Pi. Omitting a suffix means
                the value is in bytes. For the ServiceBackend, the values ‘lowmem’, ‘standard’, and ‘highmem’ are also
                valid arguments. ‘lowmem’ corresponds to approximately 1 Gi/core, ‘standard’ corresponds to
                approximately 4 Gi/core, and ‘highmem’ corresponds to approximately 7 Gi/core. The default value
                is ‘standard’.

        """
        self._memory = memory
        return self

    def storage(self, storage):
        """Set the disk size for this Step.

        storage (str, int): Disk size. The storage expression must be of the form {number}{suffix} where valid
            optional suffixes are K, Ki, M, Mi, G, Gi, T, Ti, P, and Pi. Omitting a suffix means the value is in
            bytes. For the ServiceBackend, jobs requesting one or more cores receive 5 GiB of storage for the root
            file system /. Jobs requesting a fraction of a core receive the same fraction of 5 GiB of storage.
            If you need additional storage, you can explicitly request more storage using this method and the extra
            storage space will be mounted at /io. Batch automatically writes all ResourceFile to /io.
            The default storage size is 0 Gi. The minimum storage size is 0 Gi and the maximum storage size is
            64 Ti. If storage is set to a value between 0 Gi and 10 Gi, the storage request is rounded up to 10 Gi.
            All values are rounded up to the nearest Gi.
        """
        self._storage = storage
        return self

    def always_run(self, always_run):
        """Set the always_run parameter for this Step.
        Args:
            always_run (bool): Set the Step to always run, even if dependencies fail.
        """
        self._always_run = always_run
        return self

    def timeout(self, timeout):
        """Set the timeout for this Step.

        Args:
            timeout (float, int): Set the maximum amount of time this job can run for before being killed.
        """
        self._timeout = timeout
        return self

    def _transfer_step(self):
        """Submit this Step to the Batch backend. This method is only called if the Step isn't skipped."""

        # create Batch Job object
        batch = self._pipeline._batch
        if self._reuse_job_from_previous_step:
            # reuse previous Job
            if self._reuse_job_from_previous_step._job is None:
                raise Exception(f"self._reuse_job_from_previous_step._job object is None")

            self._job = self._reuse_job_from_previous_step._job
        else:
            # create new job
            if self._step_type == _BatchStepType.PYTHON:
                self._job = batch.new_python_job(name=self.short_name)
            elif self._step_type == _BatchStepType.BASH:
                self._job = batch.new_bash_job(name=self.short_name)
            else:
                raise ValueError(f"Unexpected _BatchStepType: {self._step_type}")

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

        if self._storage:
            self._job.storage(self._storage)

        if self._timeout is not None:
            self._job.timeout(self._timeout)

        if self._always_run:
            self._job.always_run(self._always_run)

        # transfer job dependencies
        for upstream_step in self._upstream_steps:
            if upstream_step._job:
                self._job.depends_on(upstream_step._job)

        # transfer inputs
        for input_spec in self._inputs:
            print(" "*4 + f"Input: {input_spec.source_path}  ({input_spec.localize_by})")
            self._transfer_input(input_spec)

        # transfer commands
        if self._write_commands_to_script:
            # write to script
            args = self._pipeline.parse_args()

            script_lines = []
            # set bash options for easier debugging and to make command execution more robust
            script_lines.append("set -euxo pipefail")
            for command in self._commands:
                script_lines.append(command)

            script_file = tempfile.NamedTemporaryFile("wt", prefix="script_", suffix=".sh", encoding="UTF-8", delete=True)
            script_file.writelines(script_lines)
            script_file.flush()

            # upload script to the temp bucket or output dir
            script_temp_gcloud_path = os.path.join(
                args.batch_remote_tmpdir,
                f"batch_{self._unique_batch_id}/job_{self._unique_job_id}",
                os.path.basename(script_file.name))

            os.chmod(script_file.name, mode=stat.S_IREAD | stat.S_IEXEC)
            script_file_upload_command = self._generate_gsutil_copy_command(script_file.name, script_temp_gcloud_path)
            os.system(script_file_upload_command)
            script_file.close()

            script_input_spec = self.input(script_temp_gcloud_path)
            self._transfer_input(script_input_spec)
            self._job.command(f"bash -c '{script_input_spec.local_path}'")
        else:
            for command in self._commands:
                print(" "*4 + f"Adding command: {command}")
                self._job.command(command)

        # transfer outputs
        for output_spec in self._outputs:
            self._transfer_output(output_spec)
            print(" "*4 + f"Output: {output_spec}  ({output_spec.delocalize_by})")

        # clean up any files that were copied to the temp bucket
        if self._paths_localized_via_temp_bucket:
            cleanup_job_name = f"{self.short_name} cleanup {len(self._paths_localized_via_temp_bucket)} files"
            cleanup_job = self._pipeline._batch.new_job(name=cleanup_job_name)
            cleanup_job.depends_on(self._job)
            cleanup_job.always_run()
            for temp_file_path in self._paths_localized_via_temp_bucket:
                cleanup_job.command(f"gsutil -m rm -r {temp_file_path}")
            self._paths_localized_via_temp_bucket = set()

    def _get_supported_localize_by_choices(self):
        """Returns the set of Localize options supported by _BatchStep"""

        return super()._get_supported_localize_by_choices() | {
            Localize.COPY,
            Localize.GSUTIL_COPY,
            Localize.HAIL_BATCH_GCSFUSE,
            Localize.HAIL_BATCH_GCSFUSE_VIA_TEMP_BUCKET,
        }

    def _get_supported_delocalize_by_choices(self):
        """Returns the set of Delocalize options supported by _BatchStep"""

        return super()._get_supported_delocalize_by_choices() | {
            Delocalize.COPY,
            Delocalize.GSUTIL_COPY,
        }

    def _preprocess_input(self, input_spec):
        """This method is called by step.input(..) immediately when the input is first specified, regardless of whether
        the Step runs or not. It validates the input_spec's localize_by value and adds any commands to the
        Step necessary for performing this localization.

        Args:
            input_spec (_InputSpec): The input to localize.
        """

        super()._preprocess_input(input_spec)

        if input_spec.localize_by == Localize.GSUTIL_COPY:
            if not input_spec.source_path.startswith("gs://"):
                raise ValueError(f"Expected gs:// path but instead found '{input_spec.local_dir}'")
            self.command(f"mkdir -p '{input_spec.local_dir}'")
            self.command(self._generate_gsutil_copy_command(input_spec.source_path, input_spec.local_dir))
            self.command(f"ls -lh '{input_spec.local_path}'")   # check that file was copied successfully

        elif input_spec.localize_by in (
                Localize.COPY,
                Localize.HAIL_BATCH_GCSFUSE,
                Localize.HAIL_BATCH_GCSFUSE_VIA_TEMP_BUCKET):
            pass  # these will be handled in _transfer_input(..)
        elif input_spec.localize_by not in super()._get_supported_localize_by_choices():
            raise ValueError(f"The hail Batch backend doesn't support input_spec.localize_by={input_spec.localize_by}")

    def _transfer_input(self, input_spec):
        """When a Step isn't skipped and is being transferred to the execution backend, this method is called for
        each input to the Step. It performs the Steps necessary for localizing this input.

        Args:
            input_spec (_InputSpec): The input to localize.
        """
        super()._transfer_input(input_spec)

        args = self._pipeline.parse_args()
        if args.acceptable_storage_regions:
            check_gcloud_storage_region(
                input_spec.source_path,
                expected_regions=args.acceptable_storage_regions,
                gcloud_project=args.gcloud_project,
                verbose=args.verbose)

        if input_spec.localize_by == Localize.GSUTIL_COPY:
            pass  # All necessary steps for this option were already handled by self._preprocess_input(..)
        elif input_spec.localize_by == Localize.COPY:
            input_spec.read_input_obj = self._job._batch.read_input(input_spec.source_path)
            if self._step_type == _BatchStepType.BASH:
                self._job.command(f"mkdir -p '{input_spec.local_dir}'")
                self._job.command(f"ln -s {input_spec.read_input_obj} {input_spec.local_path}")
        elif input_spec.localize_by in (
            Localize.HAIL_BATCH_GCSFUSE,
            Localize.HAIL_BATCH_GCSFUSE_VIA_TEMP_BUCKET):
            self._handle_input_transfer_using_gcsfuse(input_spec)
        elif input_spec.localize_by == Localize.HAIL_HADOOP_COPY:
            self._add_commands_for_hail_hadoop_copy(input_spec.source_path, input_spec.local_dir)

    def _generate_gsutil_copy_command(self, source_path, output_dir):
        """Utility method that puts together the gsutil command for copying the given source path to an output directory.

        Args:
            source_path (str): The source path.
            output_dir (str): Output directory.

        Return:
            str: gsutil command string
        """
        args = self._pipeline.parse_args()
        gsutil_command = f"gsutil"
        if args.gcloud_project:
            gsutil_command += f" -u {args.gcloud_project}"

        output_dir = output_dir.rstrip("/") + "/"
        return f"time {gsutil_command} -m cp -r '{source_path}' '{output_dir}'"

    def _handle_input_transfer_using_gcsfuse(self, input_spec):
        """Utility method that implements localizing an input via gcsfuse.

        Args:
            input_spec (_InputSpec): The input to localize.
        """
        args = self._pipeline.parse_args()

        source_path = input_spec.source_path
        source_path_without_protocol = input_spec.source_path_without_protocol

        localize_by = input_spec.localize_by
        if localize_by == Localize.HAIL_BATCH_GCSFUSE_VIA_TEMP_BUCKET:
            if not args.batch_remote_tmpdir:
                raise ValueError("--batch-remote-tmpdir not specified.")

            temp_dir = os.path.join(
                args.batch_remote_tmpdir,
                f"batch_{self._unique_batch_id}/job_{self._unique_job_id}",
                source_path_without_protocol.strip("/")+"/")
            temp_file_path = os.path.join(temp_dir, input_spec.filename)

            if temp_file_path in self._paths_localized_via_temp_bucket:
                raise ValueError(f"{source_path} has already been localized via temp bucket.")
            self._paths_localized_via_temp_bucket.add(temp_file_path)

            # copy file to temp bucket
            self._job.command(self._generate_gsutil_copy_command(source_path, temp_dir))
        else:
            subdir = localize_by.get_subdir_name()
            source_bucket = input_spec.source_bucket

        local_root_dir = self._pipeline._get_localization_root_dir(localize_by)
        local_mount_dir = os.path.join(local_root_dir, subdir, source_bucket)
        if source_bucket not in self._buckets_mounted_via_gcsfuse:
            self._job.command(f"mkdir -p {local_mount_dir}")
            self._job.gcsfuse(source_bucket, local_mount_dir, read_only=True)
            self._buckets_mounted_via_gcsfuse.add(source_bucket)

    def _add_commands_for_hail_hadoop_copy(self, source_path, output_dir):
        """Utility method that implements localizing an input via hl.hadoop_copy.

        Args:
            source_path (str): The source path.
            output_dir (str): Output directory.
        """

        #if not hasattr(self, "_already_installed_hail"):
        #    self.command("python3 -m pip install hail")
        #self._already_installed_hail = True

        #self.command(f"mkdir -p {output_dir}")

        self.command(f"""python3 <<EOF
import hail as hl
hl.init(log='/dev/null', quiet=True)
hl.hadoop_copy("{source_path}", "{output_dir}")
EOF""")

    def _preprocess_output(self, output_spec):
        """This method is called by step.output(..) immediately when the output is first specified, regardless of
        whether the Step runs or not. It validates the output_spec.

        Args:
            output_spec (_OutputSpec): The output to preprocess.
        """
        if output_spec.delocalize_by not in self._get_supported_delocalize_by_choices():
            raise ValueError(f"Unexpected output_spec.delocalize_by value: {output_spec.delocalize_by}")

        super()._preprocess_output(output_spec)
        if not output_spec.output_dir.startswith("gs://"):
            raise ValueError(f"{output_spec.output_dir} Destination path must start with gs://")
        if output_spec.delocalize_by == Delocalize.COPY:
            # validate path since Batch delocalization doesn't work for gs:// paths with a Local backend.
            if output_spec.output_path.startswith("gs://") and self._pipeline.get_backend_type() == Backend.HAIL_BATCH_LOCAL:
                raise ValueError("The hail Batch Local backend doesn't support Delocalize.COPY for gs:// paths")
        elif output_spec.delocalize_by == Delocalize.GSUTIL_COPY:
            self.command(self._generate_gsutil_copy_command(output_spec.local_path, output_spec.output_dir))

    def _transfer_output(self, output_spec):
        """When a Step isn't skipped and is being transferred to the execution backend, this method is called for
        each output of the Step. It performs the steps necessary to delocalize the output using the approach requested
        by the user via the delocalize_by parameter.

        Args:
            output_spec (_OutputSpec): The output to delocalize.
        """
        super()._transfer_output(output_spec)

        if output_spec.delocalize_by == Delocalize.COPY:
            self._output_file_counter += 1
            output_file_obj = self._job[f"ofile{self._output_file_counter}"]
            self._job.command(f'cp {output_spec.local_path} {output_file_obj}')
            self._job._batch.write_output(output_file_obj, output_spec.output_dir.rstrip("/") + f"/{output_spec.output_filename}")
        elif output_spec.delocalize_by == Delocalize.GSUTIL_COPY:
            pass  # GSUTIL_COPY was already handled in _preprocess_output(..)
        elif output_spec.delocalize_by == Delocalize.HAIL_HADOOP_COPY:
            self.command(self._add_commands_for_hail_hadoop_copy(output_spec.local_path, output_spec.output_dir))

