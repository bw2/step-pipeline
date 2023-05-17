"""This module contains Hail Batch-specific extensions of the Pipeline and Step classes"""

import os
import stat
import tempfile
from enum import Enum

import hailtop.batch as hb

from .constants import Backend
from .io import InputSpec, InputValueSpec, InputType
from .pipeline import Pipeline, Step, Localize, Delocalize
from .utils import check_gcloud_storage_region

# TODO get latest tag via https://hub.docker.com/v2/repositories/hailgenetics/genetics/tags/ ?
DEFAULT_BASH_IMAGE = DEFAULT_PYTHON_IMAGE = "hailgenetics/hail:0.2.77"


class BatchStepType(Enum):
    """Constants that represent different Batch Step types."""
    PYTHON = "python"
    BASH = "bash"


class BatchPipeline(Pipeline):
    """This class contains Hail Batch-specific extensions of the Pipeline class"""

    def __init__(self, name=None, config_arg_parser=None, backend=Backend.HAIL_BATCH_SERVICE):
        """
        BatchPipeline constructor

        Args:
            name (str): Pipeline name
            config_arg_parser (configargparse): The configargparse.ArgumentParser object to use for defining
                command-line args
            backend (Backend): Either Backend.HAIL_BATCH_SERVICE or Backend.HAIL_BATCH_LOCAL
        """
        super().__init__(name=name, config_arg_parser=config_arg_parser)

        batch_args = config_arg_parser.add_argument_group("hail batch")
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

        args = self.parse_known_args()

        self._backend = backend
        self._gcloud_project = args.gcloud_project
        self._cancel_after_n_failures = None
        self._default_image = DEFAULT_BASH_IMAGE
        self._default_python_image = DEFAULT_PYTHON_IMAGE
        self._default_memory = None
        self._default_cpu = None
        self._default_storage = None
        self._default_timeout = None
        self._default_custom_machine_type = None
        self._default_custom_machine_is_preemptible = None
        self._backend_obj = None

    @property
    def backend(self):
        """Returns either Backend.HAIL_BATCH_SERVICE or Backend.HAIL_BATCH_LOCAL"""
        return self._backend

    def new_step(
        self,
        name=None,
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
        """Creates a new pipeline Step.

        Args:
            name (str): A short name for this Step.
            step_number (int): Optional Step number which serves as another alias for this step in addition to name.
            arg_suffix (str): Optional suffix for the command-line args that will be created for forcing or skipping
                execution of this Step.
            depends_on (Step): Optional upstream Step that this Step depends on.
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
            reuse_job_from_previous_step (Step): Optionally, reuse the batch.Job object from this other upstream Step.
            localize_by (Localize): If specified, this will be the default Localize approach used by Step inputs.
            delocalize_by (Delocalize): If specified, this will be the default Delocalize approach used by Step outputs.
        Return:
            BatchStep: The new BatchStep object.
        """

        step = BatchStep(
            self,
            name=name,
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
            step.depends_on(depends_on)

        # register the Step
        self._all_steps.append(step)

        return step

    def gcloud_project(self, gcloud_project):
        """Set the requester-pays project.

        Args:
            gcloud_project (str): The name of the Google Cloud project to be billed when accessing requester-pays
                buckets.
        """
        self._gcloud_project = gcloud_project
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
            default_timeout (int): Maximum time in seconds for a job to run before being killed. Only applicable for the
                ServiceBackend. If None, there is no timeout.
        """
        self._default_timeout = default_timeout
        return self

    def default_custom_machine_type(self, default_custom_machine_type):
        """Set the default custom machine type.

        Args:
            default_custom_machine_type (str): Cloud machine type, eg. 'n1-highmem-32'
        """
        self._default_custom_machine_type = default_custom_machine_type
        return self

    def default_custom_machine_is_preemptible(self, default_custom_machine_is_preemptible):
        """Set whether the custom machine should be preemptible.

        Args:
            default_custom_machine_is_preemptible (bool): Whether the custom machine is preemptible.
        """
        self._default_custom_machine_is_preemptible = default_custom_machine_is_preemptible
        return self

    def run(self):
        """Batch-specific code for submitting the pipeline to the Hail Batch backend"""
        print(f"Starting {self.name or ''} pipeline:")

        super().run()

        try:
            self._create_batch_obj()

            num_steps_transferred = self._transfer_all_steps()

            if num_steps_transferred == 0:
                print("No steps to run. Exiting..")
                return

            result = self._run_batch_obj()
            return result
        finally:
            if isinstance(self._backend_obj, hb.ServiceBackend):
                self._backend_obj.close()

    def _get_localization_root_dir(self, localize_by):
        """Return the top-level root directory where localized files will be copied"""
        return "/io"

    def _create_batch_obj(self):
        """Instantiate the Hail Batch Backend."""

        args = self.parse_known_args()

        if self._backend == Backend.HAIL_BATCH_LOCAL:
            self._backend_obj = hb.LocalBackend()
        elif self._backend == Backend.HAIL_BATCH_SERVICE:
            if not args.batch_billing_project:
                raise ValueError("--batch-billing-project must be set when --cluster is used")
            if not args.batch_remote_tmpdir:
                raise ValueError("--batch-remote-tmpdir must be set when --cluster is used")
            self._backend_obj = hb.ServiceBackend(
                google_project=args.gcloud_project,
                billing_project=args.batch_billing_project,
                remote_tmpdir=args.batch_remote_tmpdir)
        else:
            raise Exception(f"Unexpected _backend: {self._backend}")

        self._batch = hb.Batch(
            backend=self._backend_obj,
            name=self.name,
            requester_pays_project=self._gcloud_project,  # The name of the Google project to be billed when accessing requester pays buckets.
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

        if self._backend == Backend.HAIL_BATCH_LOCAL:
            # Hail Batch LocalBackend mode doesn't support some of the args suported by ServiceBackend
            result = self._batch.run(dry_run=args.dry_run, verbose=args.verbose)
        elif self._backend == Backend.HAIL_BATCH_SERVICE:
            result = self._batch.run(
                dry_run=args.dry_run,
                verbose=False,  # always set to False since hail verbose output is too detailed
                delete_scratch_on_exit=None,  # If True, delete temporary directories with intermediate files
                wait=True,  # If True, wait for the batch to finish executing before returning
                open=False,  # If True, open the UI page for the batch
                disable_progress_bar=False,  # If True, disable the progress bar.
                callback=None,  # If not None, a URL that will receive at most one POST request after the entire batch completes.
            )
        else:
            raise Exception(f"Unexpected _backend: {self._backend}")

        # The Batch pipeline returns an undocumented result object which can be used to retrieve the Job's status code
        # and logs
        return result

    def _transfer_all_steps(self):
        """This method performs the core task of executing a pipeline. It traverses the execution graph (DAG) of
        user-defined Steps and decides which steps can be skipped, and which should be executed (ie. transferred to
        the execution backend).
        """

        num_steps_transferred = super()._transfer_all_steps()

        # handle --slack-when-done by adding an always-run job
        args = self.parse_known_args()
        if args.slack_when_done and num_steps_transferred > 0:
            post_to_slack_job = self._batch.new_job(name="post to slack when done")
            for step in self._all_steps:
                if step._job:
                    post_to_slack_job.depends_on(step._job)
            post_to_slack_job.always_run()
            post_to_slack_job.cpu(0.25)
            slack_message = f"{self.name} pipeline finished"
            post_to_slack_job.command("python3 -m pip install slacker")
            post_to_slack_job.command(self._generate_post_to_slack_command(slack_message))

        return num_steps_transferred


class BatchStep(Step):
    """This class contains Hail Batch-specific extensions of the Step class"""

    def __init__(
        self,
        pipeline,
        name=None,
        step_number=None,
        arg_suffix=None,
        image=None,
        cpu=None,
        memory=None,
        storage=None,
        always_run=False,
        timeout=None,
        custom_machine_type=None,
        custom_machine_is_preemptible=None,
        output_dir=None,
        reuse_job_from_previous_step=None,
        localize_by=Localize.COPY,
        delocalize_by=Delocalize.COPY,
    ):
        """Step constructor.

        Args:
            pipeline (BatchPipeline): The pipeline that this Step is a part of.
            name (str): Step name
            step_number (int): optional Step number which serves as another alias for this step in addition to name.
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
            custom_machine_type (str):
            custom_machine_is_preemptible (bool):
            output_dir (str): Optional default output directory for Step outputs.
            reuse_job_from_previous_step (Step): Optionally, reuse the batch.Job object from this other upstream Step.
            localize_by (Localize): If specified, this will be the default Localize approach used by Step inputs.
            delocalize_by (Delocalize): If specified, this will be the default Delocalize approach used by Step outputs.
        """
        super().__init__(
            pipeline,
            name,
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
        self._custom_machine_type = custom_machine_type
        self._custom_machine_is_preemptible = custom_machine_is_preemptible
        self._reuse_job_from_previous_step = reuse_job_from_previous_step

        self._job = None
        self._output_file_counter = 0

        self._paths_localized_via_temp_bucket = set()
        self._buckets_mounted_via_cloudfuse = set()

        self._step_type = BatchStepType.BASH
        self._write_commands_to_script = False

        self._regions = None
        self._localize_by_copy_already_created_dirs_set = set()

    def regions(self, *region):
        """Set one or more compute regions.

        Args:
            region (str): eg. "us-central1".
        """
        self._regions = region

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

        Args:
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

    def custom_machine_type(self, custom_machine_type):
        """Set a custom machine type.

        Args:
            custom_machine_type (str): Cloud machine type, eg. 'n1-highmem-32'
        """
        self._custom_machine_type = custom_machine_type
        return self

    def custom_machine_is_preemptible(self, custom_machine_is_preemptible):
        """Set whether the custom machine should be preemptible.

        Args:
            custom_machine_is_preemptible (bool): Whether the custom machine is preemptible.
        """
        self._custom_machine_is_preemptible = custom_machine_is_preemptible
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
            if self._step_type == BatchStepType.PYTHON:
                self._job = batch.new_python_job(name=self.name)
            elif self._step_type == BatchStepType.BASH:
                self._job = batch.new_bash_job(name=self.name)
            else:
                raise ValueError(f"Unexpected BatchStepType: {self._step_type}")

        # set execution parameters
        if self._image:
            self._job.image(self._image)

        if self._regions is not None:
            self._job.regions(self._regions)
        else:
            # set the default region to us-central1 to avoid random egress charges
            self._job.regions(["us-central1"])

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

        custom_machine_type_requested = any(p is not None for p in [
            self._custom_machine_type, self._custom_machine_is_preemptible,
            self._pipeline._default_custom_machine_type, self._pipeline._default_custom_machine_is_preemptible,
        ])
        if custom_machine_type_requested:
            if self._cpu or self._memory:
                raise ValueError("Both a custom_machine_type or custom_machine_is_preemptible as well as cpu or memory "
                                 "arguments were specified. Only one or the other should be provided.")
            self.job._machine_type = self._custom_machine_type or self._pipeline._default_custom_machine_type
            #if self._custom_machine_is_preemptible is not None or self._default_custom_machine_is_preemptible is not None:
            self.job._preemptible = self._custom_machine_is_preemptible or self._pipeline._default_custom_machine_is_preemptible

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
        for input_spec in self._input_specs:
            print(" "*4 + f"Input: {input_spec.original_source_path}  ({input_spec.localize_by})")
            self._transfer_input_spec(input_spec)

        # transfer commands
        if self._write_commands_to_script or len(" ".join(self._commands)) > 5*10**4:
            args = self._pipeline.parse_args()

            # write script to a temp file
            script_file = tempfile.NamedTemporaryFile("wt", prefix="script_", suffix=".sh", encoding="UTF-8", delete=True)
            for command in self._commands:
                script_file.write(f"{command}\n")
            script_file.flush()

            # upload script to the temp bucket or output dir
            script_temp_gcloud_path = os.path.join(
                args.batch_remote_tmpdir,
                f"pipeline_{self._pipeline._unique_pipeline_instance_id}/step_{self._unique_step_instance_id}",
                os.path.basename(script_file.name))

            os.chmod(script_file.name, mode=stat.S_IREAD | stat.S_IEXEC)
            script_file_upload_command = self._generate_gsutil_copy_command(
                script_file.name, output_dir=os.path.dirname(script_temp_gcloud_path))
            os.system(script_file_upload_command)
            script_file.close()
            print(" "*4 + f"Will run commands from: {script_temp_gcloud_path}")
            script_input_obj = self._job._batch.read_input(script_temp_gcloud_path)
            self._job.command(f"bash -c 'source {script_input_obj}'")
        else:
            for command in self._commands:
                command_summary = command
                command_summary_line_count = len(command_summary.split("\n"))
                if command_summary_line_count > 5:
                    command_summary = "\n".join(command_summary.split("\n")[:5]) + f"\n...  {command_summary_line_count-5} more line(s)"
                print(" "*4 + f"Adding command: {command_summary}")
                self._job.command(command)

        # transfer outputs
        for output_spec in self._output_specs:
            self._transfer_output_spec(output_spec)
            print(" "*4 + f"Output: {output_spec}  ({output_spec.delocalize_by})")

        # clean up any files that were copied to the temp bucket
        if self._paths_localized_via_temp_bucket:
            cleanup_job_name = f"clean up {len(self._paths_localized_via_temp_bucket)} files"
            if self.name:
                cleanup_job_name += f" from {self.name}"
            cleanup_job = self._pipeline._batch.new_job(name=cleanup_job_name)
            cleanup_job.image("docker.io/hailgenetics/genetics:0.2.77")
            cleanup_job.depends_on(self._job)
            cleanup_job.always_run()
            cleanup_job.command("set -x")
            cleanup_job.command(f"gcloud auth activate-service-account --key-file /gsa-key/key.json")
            for temp_file_path in self._paths_localized_via_temp_bucket:
                cleanup_job.command(f"gsutil -m rm -r {temp_file_path}")
            self._paths_localized_via_temp_bucket = set()

    def _get_supported_localize_by_choices(self):
        """Returns the set of Localize options supported by BatchStep"""

        return super()._get_supported_localize_by_choices() | {
            Localize.COPY,
            Localize.GSUTIL_COPY,
            Localize.HAIL_BATCH_CLOUDFUSE,
            #Localize.HAIL_BATCH_CLOUDFUSE_VIA_TEMP_BUCKET,
        }

    def _get_supported_delocalize_by_choices(self):
        """Returns the set of Delocalize options supported by BatchStep"""

        return super()._get_supported_delocalize_by_choices() | {
            Delocalize.COPY,
            Delocalize.GSUTIL_COPY,
        }

    def _preprocess_input_spec(self, input_spec):
        """This method is called by step.input(..) immediately when the input is first specified, regardless of whether
        the Step runs or not. It validates the input_spec's localize_by value and adds any commands to the
        Step necessary for performing this localization.

        Args:
            input_spec (InputSpec): The input to localize.
        """

        input_spec = super()._preprocess_input_spec(input_spec)

        if input_spec.localize_by == Localize.GSUTIL_COPY:
            if not input_spec.original_source_path.startswith("gs://"):
                raise ValueError(f"Expected gs:// path but instead found '{input_spec.local_dir}'")
            self.command(f"mkdir -p '{input_spec.local_dir}'")
            self.command(self._generate_gsutil_copy_command(
                input_spec.original_source_path, output_dir=input_spec.local_dir))
            self.command(f"ls -lh '{input_spec.local_path}'")   # check that file was copied successfully

        elif input_spec.localize_by in (
                Localize.COPY,
                Localize.HAIL_BATCH_CLOUDFUSE):
            pass  # these will be handled in _transfer_input_spec(..)
        elif input_spec.localize_by == Localize.HAIL_BATCH_CLOUDFUSE_VIA_TEMP_BUCKET:
            raise ValueError("Localize.HAIL_BATCH_CLOUDFUSE_VIA_TEMP_BUCKET is no longer supported due to changes in gcloud egress charges")

            args = self._pipeline.parse_known_args()
            source_path = input_spec.source_path
            source_path_without_protocol = input_spec.source_path_without_protocol

            if not args.batch_remote_tmpdir:
                raise ValueError("--batch-remote-tmpdir not specified.")

            temp_dir = os.path.join(
                args.batch_remote_tmpdir,
                f"pipeline_{self._pipeline._unique_pipeline_instance_id}/step_{self._unique_step_instance_id}",
                os.path.dirname(source_path_without_protocol).strip("/")+"/")
            temp_file_path = os.path.join(temp_dir, input_spec.filename)

            if temp_file_path in self._paths_localized_via_temp_bucket:
                raise ValueError(f"{source_path} has already been localized via temp bucket.")
            self._paths_localized_via_temp_bucket.add(temp_file_path)

            # copy file to temp bucket
            gsutil_command = self._generate_gsutil_copy_command(source_path, output_dir=temp_dir)
            self.command(gsutil_command)

            # create an InputSpec with the updated source path
            input_spec = InputSpec(
                source_path=temp_file_path,
                name=input_spec.name,
                localize_by=input_spec.localize_by,
                localization_root_dir=input_spec.localization_root_dir,
                original_source_path=input_spec.source_path,
            )

        elif input_spec.localize_by not in super()._get_supported_localize_by_choices():
            raise ValueError(
                f"The hail Batch backend doesn't support input_spec.localize_by={input_spec.localize_by}")

        return input_spec

    def _transfer_input_spec(self, input_spec):
        """When a Step isn't skipped and is being transferred to the execution backend, this method is called for
        each input to the Step. It performs the steps necessary to localize this input.

        Args:
            input_spec (InputSpec): The input to localize.
        """
        super()._transfer_input_spec(input_spec)

        args = self._pipeline.parse_known_args()
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
            if self._step_type == BatchStepType.BASH:
                self._job.command(f"touch {input_spec.read_input_obj}")   # needed to trigger download

                echo_done_command = 'echo "Done localizing files via COPY"'
                commands_to_set_up_path = []
                if not input_spec.local_dir in self._localize_by_copy_already_created_dirs_set:
                    commands_to_set_up_path.append(f"mkdir -p '{input_spec.local_dir}'")
                    self._localize_by_copy_already_created_dirs_set.add(input_spec.local_dir)
                commands_to_set_up_path.append(f"ln -s {input_spec.read_input_obj} {input_spec.local_path}")

                try:
                    echo_done_command_index = self._commands.index(echo_done_command)
                except ValueError:
                    echo_done_command_index = 0
                    commands_to_set_up_path.append(echo_done_command)

                for command_to_set_up_path in commands_to_set_up_path[::-1]:
                    self._commands.insert(echo_done_command_index, command_to_set_up_path)
                
        elif input_spec.localize_by in (
            Localize.HAIL_BATCH_CLOUDFUSE,
            Localize.HAIL_BATCH_CLOUDFUSE_VIA_TEMP_BUCKET):
            self._handle_input_transfer_using_cloudfuse(input_spec)
        elif input_spec.localize_by == Localize.HAIL_HADOOP_COPY:
            self._add_commands_for_hail_hadoop_copy(input_spec.source_path, input_spec.local_dir)

    def _generate_gsutil_copy_command(self, source_path, output_dir=None, output_path=None):
        """Utility method that puts together the gsutil command for copying the given source path to an output path
        or directory. Either the output path or the output directory must be provided.

        Args:
            source_path (str): The source path.
            output_dir (str): Output directory.
            output_path (str): Output file path.
        Return:
            str: gsutil command string
        """
        args = self._pipeline.parse_known_args()
        gsutil_command = f"gsutil"
        if args.gcloud_project:
            gsutil_command += f" -u {args.gcloud_project}"

        if output_path:
            destination = output_path
        elif output_dir:
            destination = output_dir.rstrip("/") + "/"
        else:
            raise ValueError("Neither output_path nor output_dir arg was specified")

        return f"time {gsutil_command} -m cp -r '{source_path}' '{destination}'"

    def _handle_input_transfer_using_cloudfuse(self, input_spec):
        """Utility method that implements localizing an input via cloudfuse.

        Args:
            input_spec (InputSpec): The input to localize.
        """
        localize_by = input_spec.localize_by
        source_bucket = input_spec.source_bucket
        local_root_dir = self._pipeline._get_localization_root_dir(localize_by)
        local_mount_dir = os.path.join(local_root_dir, localize_by.get_subdir_name(), source_bucket)
        if source_bucket not in self._buckets_mounted_via_cloudfuse:
            self._job.command(f"mkdir -p {local_mount_dir}")
            self._job.cloudfuse(source_bucket, local_mount_dir, read_only=True)
            self._buckets_mounted_via_cloudfuse.add(source_bucket)

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

    def _preprocess_output_spec(self, output_spec):
        """This method is called by step.output(..) immediately when the output is first specified, regardless of
        whether the Step runs or not. It validates the output_spec.

        Args:
            output_spec (OutputSpec): The output to preprocess.
        """
        if output_spec.delocalize_by not in self._get_supported_delocalize_by_choices():
            raise ValueError(f"Unexpected output_spec.delocalize_by value: {output_spec.delocalize_by}")

        super()._preprocess_output_spec(output_spec)
        if output_spec.delocalize_by == Delocalize.COPY:
            # validate path since Batch delocalization doesn't work for gs:// paths with a Local backend.
            if self._pipeline.backend == Backend.HAIL_BATCH_LOCAL and any(
                    output_spec.output_path.startswith(s) for s in ("gs://", "hail-az://")):
                raise ValueError("The hail Batch Local backend doesn't support Delocalize.COPY for non-local path: "
                                 f"{output_spec.output_path}")
            if not output_spec.filename:
                raise ValueError(f"{output_spec} filename isn't specified. It is required for Delocalize.COPY")
        elif output_spec.delocalize_by == Delocalize.GSUTIL_COPY:
            if not output_spec.output_path.startswith("gs://"):
                raise ValueError(f"{output_spec.output_path} Destination path must start with gs://")

            if not hasattr(self, "_switched_gcloud_auth_to_user_account"):
                self.gcloud_auth_activate_service_account()
            self.command(self._generate_gsutil_copy_command(output_spec.local_path, output_path=output_spec.output_path))

    def _transfer_output_spec(self, output_spec):
        """When a Step isn't skipped and is being transferred to the execution backend, this method is called for
        each output of the Step. It performs the steps necessary to delocalize the output using the approach requested
        by the user via the delocalize_by parameter.

        Args:
            output_spec (OutputSpec): The output to delocalize.
        """
        super()._transfer_output_spec(output_spec)

        if output_spec.delocalize_by == Delocalize.COPY:
            self._output_file_counter += 1
            output_file_obj = self._job[f"ofile{self._output_file_counter}"]
            self._job.command(f'cp {output_spec.local_path} {output_file_obj}')

            if not output_spec.output_dir:
                raise ValueError(f"{output_spec} output directory is required for Delocalize.COPY")
            if not output_spec.filename:
                raise ValueError(f"{output_spec} output filename is required for Delocalize.COPY")

            destination_path = os.path.join(output_spec.output_dir, output_spec.filename)
            self._job.command(f'echo Copying {output_spec.local_path} to {destination_path}')
            self._job._batch.write_output(output_file_obj, destination_path)

        elif output_spec.delocalize_by == Delocalize.GSUTIL_COPY:
            pass  # GSUTIL_COPY was already handled in _preprocess_output_spec(..)
        elif output_spec.delocalize_by == Delocalize.HAIL_HADOOP_COPY:
            self.command(self._add_commands_for_hail_hadoop_copy(output_spec.local_path, output_spec.output_dir))

