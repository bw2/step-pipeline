"""
The core object in this library is a "Step" which encapsulates a pipeline job or task.
A single Step represents a set of commands which together produce output file(s), and which can be
skipped if the output files already exist (and are newer than the input files).

A Step object, besides checking input and output files to decide if it needs to run, is responsible for:
- localizing/delocalizing the input/output files
- defining argparse args for skipping or forcing execution
- optionally, collecting timing and profiling info on cpu, memory & disk-use while the Step is running. It
does this by adding a background process to the container to record cpu, memory and disk usage every 10 seconds,
and to localize/delocalize a .tsv file that accumulates these stats across all steps being profiled.

For concreteness, the docs below are written with Hail Batch as the backend. However, this library is designed
to eventually accommodate other backends - such as "local", "cromwell", "SGE", "dsub", etc.

By default, a Step corresponds on a single Batch Job, but in some cases the same Batch Job may be reused for
multiple steps (for example, step 1 creates a VCF and step 2 tabixes it).

This library allows code that looks like:

with pipeline.step_pipeline(desc="merge vcfs and compute per-chrom counts") as sp:
     sp.add_argument("--my-flag", action="store_true", help="toggle something")
     args = sp.parse_known_args() # if needed, parse command-line args that have been defined so far

     # step 1 is automatically skipped if the output file gs://my-bucket/outputs/merged.vcf.gz already exists and is newer than all part*.vcf input files.
     s1 = bp.new_step(label="merge vcfs", memory=16)  # internally, this creates a Batch Job object and also defines new argparse args like --skip-step1
     s1.input_file_glob(input_files="gs://my-bucket/part*.vcf", localize_inputs_using_gcsfuse=False, label="input vcfs")  # internally, this will add a gsutil cp command to localize these files
     s1.output_file(output_file="gs://my-bucket/outputs/merged.vcf.gz", label="merged vcf")  # this automatically appends a gsutil command at the end to delocalize merge.vcf.gz to gs://my-bucket/outputs/merged.vcf.gz

     s1.command("python3 merge_vcfs.py part*.vcf -o temp.vcf")
     s1.command("bgzip -c temp.vcf > merged.vcf.gz")

     # step 2
     s2 = s1.new_step(label="index vcf", create_new_job=False)   # s2 reuses the Batch Job object from s1
     s2.input_file(s1.get_output_file("merged vcf"), label="merged vcf")
     s2.output_file(f"{s1.get_output_file('merged vcf')}.tbi")
     s2.command("tabix {s2.get_local_path('merged vcf')}")       # this commmand will be added to the DAG only if the output file doesn't exist yet or is older than the vcf.

     # step 3 contains a scatter-gather which is skipped if counts.tsv already exists
     s3 = s2.new_step(label="count variants per chrom")
     s3.input_files_from(s1)
     s3.input_files_from(s2)
     s3.output_file("gs://my-bucket/outputs/counts.tsv")
     counting_jobs = []
     for chrom in range(1, 23):
             s3_job = s3.new_job(cpu=0.5, label=f"counting job {chrom}")  # s3 contains multiple Jobs
             s3_job.localize(s1.output_file, use_gcsfuse=False, label="merged vcf")
             s3_job.command(f"cat {s3.get_local_path('merged vcf')} | grep '^{chrom}' | wc -l > counts_{chrom}.tsv")
             s3_job.output("counts_{chrom}.tsv")  # copied to Batch temp bucket
             counting_jobs.append(s3_job)
     s3_gather_job = s3.new_job(cpu=0.5, depends_on=counting_jobs)
     ...

# here the wrapper will parse command-line args, decide which steps to skip, pass the DAG to Batch, and then call batch.run

Ths is a summary of the main classes and utilities in the Step library:

_Step:
    - not specific to any execution backend
    - data:
        - list of inputs
        - list of outputs
        - list of commands and _Steps contained within this _Step
        - output dir
        - list of upstream steps
        - list of downstream steps
        - name
    - methods:
        - new_step(self, name, reuse_job=False, cpu=None, memory=None, disk=None)
        - command(self, command)
        - input(self, path, name=None, use_gcsfuse=False)
        - input_glob(self, glob, name=None, use_gcsfuse=False)
        - output(self, path, name=None)
        - output_dir(self, path)
        - depends_on(self, step, reuse_job=False)
        - has_upstream_steps(self)


_Pipeline
    - parent class for backend-specific classes
    - methods:
        - _transfer_step(self, step: _Step, execution_context):        - not specific to any execution backend

_BatchPipeline

utils:
    are_any_inputs_missing(step: _Step) -> bool
    are_outputs_up_to_date(step: _Step) -> bool
"""


from abc import ABC, abstractmethod

import configargparse
import os

from .utils import are_outputs_up_to_date
from .io import Localize, Delocalize, _InputSpec, _OutputSpec


class _Pipeline(ABC):
    """_Pipeline represents the execution pipeline. This class contains only generalized code that is not specific to
    any particular execution backend. It is the parent class for subclasses such as _BatchPipeline that are specific to
    a particualr execution backend.
    This class contains public methods for creating Steps. It also has some private methods that implement the
    general aspects of traversing the execution graph (DAG) and transferring all steps to a specific execution backend.
    """

    def __init__(self, name=None, config_arg_parser=None):
        """Constructor.

        Args:
            name (str): A name for the pipeline.
            config_arg_parser (configargparse.ArgumentParser): Optional instance of configargparse.ArgumentParser
                to use for defining pipeline command-line args. If not specified, a new instance will be created
                internally.
        """
        if config_arg_parser is None:
            config_arg_parser = configargparse.ArgumentParser(
                add_config_file_help=True,
                add_env_var_help=True,
                formatter_class=configargparse.HelpFormatter,
                ignore_unknown_config_file_keys=True,
                config_file_parser_class=configargparse.YAMLConfigFileParser,
            )

        self.name = name
        self._config_arg_parser = config_arg_parser
        self._all_steps = []

        config_arg_parser.add_argument("-v", "--verbose", action='count', default=0, help="Print more info")
        config_arg_parser.add_argument("-c", "--config-file", help="YAML config file path", is_config_file_arg=True)
        config_arg_parser.add_argument("--dry-run", action="store_true", help="Don't run commands, just print them.")
        config_arg_parser.add_argument("-f", "--force", action="store_true", help="Force execution of all steps.")

        grp = config_arg_parser.add_argument_group("notifications")
        grp.add_argument("--slack-token", env_var="SLACK_TOKEN", help="Slack token to use for notifications")
        grp.add_argument("--slack-channel", env_var="SLACK_CHANNEL", help="Slack channel to use for notifications")

    def get_config_arg_parser(self):
        """Returns the configargparse.ArgumentParser object used by the Pipeline to define command-line args.
        This is a drop-in replacement for argparse.ArgumentParser with some extra features such as support for
        config files and environment variables. See https://github.com/bw2/ConfigArgParse for more details.
        You can use this to add and parse your own command-line arguments the same way you would using argparse. For
        example:

        p = pipeline.get_config_arg_parser()
        p.add_argument("--my-arg")
        args = pipeline.parse_args()
        """
        return self._config_arg_parser

    def parse_args(self):
        """Parse command line args defined up to this point. This method can be called more than once.

        Return:
            argparse args object.
        """
        args, _ = self._config_arg_parser.parse_known_args(ignore_help_args=True)
        return args

    @abstractmethod
    def new_step(self, short_name, step_number=None):
        """Creates a new pipeline Step. Subclasses must implement this method.

        Args:
            short_name (str): A short name for the step.
            step_number (int): Optional step number.
        """

    @abstractmethod
    def run(self):
        """Submits a pipeline to an execution engine such as Hail Batch. Subclasses must implement this method.
        They should use this method to perform initialization of the specific execution backend and then call
        self._transfer_all_steps(..).
        """

    @abstractmethod
    def _get_localization_root_dir(self, localize_by):
        """Returns the top level directory where files will be localized to. For example /data/mounted_disk/.

        Args:
            localize_by (Localize): The approach being used to localize files.
        """

    def __enter__(self):
        """Enables create a pipeline using a python 'with ' statement - with code like:

        with pipeline() as sp:
            sp.new_step(..)
            ..
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """This method runs at the completion of a 'with' block, and is used to launch the pipeline after all Steps
         have been defined."""

        self.run()

    def _transfer_all_steps(self):
        """This method performs the core task of executing a pipeline. It tranverses the execution graph (DAG) of
        user-defined Steps and decides which steps can be skipped, and which should be executed (ie. transfered to
        the execution backend.

        To decide whether a Step needs to run, this method takes into account any --skip-* command-line args,
        --force-* command-line args, whether the Step's output files already exist and are newer than all input files,
        and whether all upstream steps are also being skipped (if not, the Step will need to run).

        For Steps that need to run, this method calls step._transfer_step() to perform any backend-specific actions
        necessary to actually run the Step.
        """

        args = self.parse_args()

        steps_to_run_next = [s for s in self._all_steps if not s.has_upstream_steps()]
        num_steps_transferred = 0
        while steps_to_run_next:
            #print("Next steps: ", steps_to_run_next)
            for step in steps_to_run_next:
                if not step._commands:
                    print(f"WARNING: No commands specified for step [{step}]. Skipping...")
                    continue

                # decide whether this step needs to run
                decided_this_step_needs_to_run = False

                skip_requested = any(
                    getattr(args, skip_arg_name.replace("-", "_")) for skip_arg_name in step._skip_this_step_arg_names
                )

                if not skip_requested:
                    is_being_forced = args.force or any(
                        getattr(args, force_arg_name.replace("-", "_")) for force_arg_name in step._force_this_step_arg_names
                    )
                    if is_being_forced:
                        decided_this_step_needs_to_run = True

                    if not decided_this_step_needs_to_run:
                        all_upstream_steps_skipped = all(s._is_being_skipped for s in step._upstream_steps)
                        if not all_upstream_steps_skipped:
                            decided_this_step_needs_to_run = True

                    if not decided_this_step_needs_to_run:
                        # only do this check if upstream steps are being skipped. Otherwise, input files may not exist yet.
                        outputs_are_up_to_date = are_outputs_up_to_date(step)
                        if not outputs_are_up_to_date:
                            decided_this_step_needs_to_run = True
                        else:
                            if args.verbose > 0:
                                print(f"Outputs are up-to-date:")
                                for o in step._outputs:
                                    print(f"       {o}")

                if decided_this_step_needs_to_run:
                    print(f"==> Running {step}")
                    step._is_being_skipped = False
                    step._transfer_step()
                    num_steps_transferred += 1
                else:
                    print(f"Skipping {step}")
                    step._is_being_skipped = True

            # next, process all steps that depend on the previously-completed steps
            steps_to_run_next = [downstream_step for step in steps_to_run_next for downstream_step in step._downstream_steps]

        return num_steps_transferred


class _Step(ABC):
    """Represents a set of commands or sub-steps which together produce some output file(s), and which can be
    skipped if the output files already exist (and are newer than any input files unless a --force arg is used).
    A Step's input and output files must be stored in some persistant location, like a local disk or GCS.

    Using Hail Batch as an example, a Step typically corresponds to a single Hail Batch Job. Sometimes a Job can be
    reused to run multiple steps (for example, where step 1 creates a VCF and step 2 tabixes it).
    """

    _USED_ARG_SUFFIXES = set()

    def __init__(self, pipeline, short_name, step_number=None, arg_suffix=None, output_dir=None,
                 localize_by=None, delocalize_by=None):
        """_Step constructor

        Args:
            pipeline (_Pipeline): The _Pipeline object representing the current pipeline.
            short_name (str): A short name for this step
            step_number (int): If specified, --skip-step{step_number} and --force-step{step_number} command-line args
                will be created.
            arg_suffix (str): If specified, --skip-{arg_suffix} and --force-{arg_suffix} command-line args will be
                created.
            output_dir (str): If specified, this will be the default output directory used by Step outputs.
            localize_by (Localize): If specified, this will be the default Localize approach used by Step inputs.
            delocalize_by (Delocalize): If specified, this will be the default Delocalize approach used by Step outputs.
        """
        self._pipeline = pipeline
        self.short_name = short_name
        self.step_number = step_number
        self.arg_suffix = arg_suffix
        self._output_dir = output_dir

        self._localize_by = localize_by
        self._delocalize_by = delocalize_by

        self._inputs = []
        self._outputs = []

        self._commands = []   # used for BashJobs

        #self._calls = []  # use for PythonJobs (Not yet implemented)
        #self._substeps = []  # steps that are contained within this step (Not yet implemented)

        self._upstream_steps = []  # this Step depends on these Steps
        self._downstream_steps = []  # Steps that depend on this Step

        self._is_being_skipped = False  # records whether this Step is being skipped

        self._force_this_step_arg_names = []
        self._skip_this_step_arg_names = []

        # define command line args for skipping or forcing execution of this step
        argument_parser = pipeline.get_config_arg_parser()

        command_line_arg_suffixes = []
        if arg_suffix:
            command_line_arg_suffixes.append(arg_suffix)
        else:
            command_line_arg_suffixes.append(short_name.replace(" ", "-").replace(":", ""))

        if step_number is not None:
            if not isinstance(step_number, (int, float)):
                raise ValueError(f"step_number must be an integer or a float rather than '{step_number}'")
            command_line_arg_suffixes.append(f"step{step_number}")

        for suffix in command_line_arg_suffixes:
            if suffix in _Step._USED_ARG_SUFFIXES:
                continue

            argument_parser.add_argument(
                f"--force-{suffix}",
                help=f"Force execution of '{short_name}'.",
                action="store_true",
            )
            self._force_this_step_arg_names.append(f"force_{suffix}")
            argument_parser.add_argument(
                f"--skip-{suffix}",
                help=f"Skip '{short_name}' even if --force is used.",
                action="store_true",
            )
            self._skip_this_step_arg_names.append(f"skip_{suffix}")
            _Step._USED_ARG_SUFFIXES.add(suffix)

    def short_name(self, short_name):
        """Set the short name for this Step.

        Args:
            short_name (str): Namee
        """
        self.short_name = short_name

    def output_dir(self, output_dir):
        """Set the default output directory for Step outputs.

        Args:
            output_dir (str): Output directory path.
        """
        self._output_dir = output_dir

    def command(self, command):
        """Add a shell command to this Step.

        Args:
            command (str): A shell command to execute as part of this Step
        """

        self._commands.append(command)

    def input_glob(self, glob, name=None, localize_by=None):
        """Specify input file(s) to this Step using glob syntax (ie. using wildcards as in gs://bucket/dir/sample*.cram)

        Args:
            glob (str): The path of the input file(s) or directory to localize, optionally including wildcards.
            name (str): Optional name for this input.
            localize_by (Localize): How this path should be localized.

        Return:
            _InputSpec: An object that describes the specified input file or directory.
        """
        return self.input(glob, name=name, localize_by=localize_by)

    def input(self, source_path, name=None, localize_by=None):
        """Specifies an input file or directory.

        Args:
            source_path (str): Path of input file or directory to localize.
            name (str): Optional name for this input.
            localize_by (Localize): How this path should be localized.

        Return:
            _InputSpec: An object that describes the specified input file or directory.
        """
        localize_by = localize_by or self._localize_by

        # validate inputs
        if not source_path.startswith("gs://") and localize_by in (
                Localize.GSUTIL_COPY,
                Localize.HAIL_BATCH_GCSFUSE,
                Localize.HAIL_BATCH_GCSFUSE_VIA_TEMP_BUCKET):
            raise ValueError(f"source_path '{source_path}' doesn't start with gs://")

        input_spec = _InputSpec(
            source_path=source_path,
            name=name,
            localize_by=localize_by,
            localization_root_dir=self._pipeline._get_localization_root_dir(localize_by),
        )

        self._preprocess_input(input_spec)
        self._inputs.append(input_spec)

        return input_spec

    def inputs(self, source_path, *source_paths, name=None, localize_by=None):
        """Specifies one or more input file or directory paths.

        Args:
            source_path (str): Path of input file or directory to localize.
            name (str): Optional name to apply to all these inputs.
            localize_by (Localize): How these paths should be localized.

        Return:
            list: A list of _InputSpec objects that describe these input files or directories. The list will contain
                one entry for each passed-in source path.
        """
        source_paths = [source_path, *source_paths]
        input_specs = []
        for source_path in source_paths:
            input_spec = self.input(source_path, name=name, localize_by=localize_by)
            input_specs.append(input_spec)

        if len(source_paths) == 1:
            return input_specs[0]
        else:
            return input_specs

    def use_the_same_inputs_as(self, other_step, localize_by=None):
        """Copy the inputs of another step, while optionally changing the localize_by approach. This is a utility method
        to make it easier to specify inputs for a Step that is very similar to a previously-defined step.

        Args:
            other_step (_Step): The Step object to copy inputs from.
            localize_by (Localize): Optionally specify how these inputs should be localized. If not specified, the value
                from other_step will be reused.

        Return:
             list: A list of new _InputSpec objects that describe the inputs copied from other_step. The returned list
                will contain one entry for each input of other_step.
        """
        localize_by = localize_by or self._localize_by

        input_specs = []
        for other_step_input_spec in other_step._inputs:
            input_spec = self.input(
                source_path=other_step_input_spec.source_path,
                name=other_step_input_spec.output_name,
                localize_by=localize_by or other_step_input_spec.localize_by,
            )
            input_specs.append(input_spec)
        return input_specs

    def use_previous_step_outputs_as_inputs(self, previous_step, localize_by=None):
        """Define Step inputs to be the output paths of an upstream Step and explicitly mark this Step as downstream of
        previous_step by calling self.depends_on(previous_step).

        Args:
            previous_step (Step): A Step that's upstream of this Step in the pipeline.
            localize_by (Localize): Specify how these inputs should be localized. If not specified, the default
                localize_by value for the pipeline will be used.
        Return:
             list: A list of new _InputSpec objects that describe the inputs defined based on the outputs of
             previous_step. The returned list will contain one entry for each output of previous_step.
        """
        self.depends_on(previous_step)

        localize_by = localize_by or self._localize_by

        input_specs = []
        for output_spec in previous_step._outputs:
            input_spec = self.input(
                source_path=output_spec.output_path,
                name=output_spec.output_name,
                localize_by=localize_by,
            )
            input_specs.append(input_spec)

        return input_specs

    def output_dir(self, path):
        """If an output path is specified as a relative path, it will be relative to this dir.

        Args:
            path (str): Directory path.
        """
        self._output_dir = path

    def output(self, local_path, output_path=None, output_dir=None, name=None, delocalize_by=None):
        """Specify a Step output file or directory.

        Args:
            local_path (str): The file or directory path within the execution container's file system.
            output_path (str): Optional destination path to which the local_path should be delocalized.
            output_dir (str): Optional destination directory to which the local_path should be delocalized.
                It is expected that either output_path will be specified, or an output_dir value will be provided as an
                argument to this method or previously (such as by calling the step.output_dir(..) setter method).
                If both output_path and output_dir are specified and output_path is a relative path, it is interpretted
                as being relative to output_dir.
            name (str): Optional name for this output.
            delocalize_by (Delocalize): How this path should be delocalized.

        Returns:
            _OutputSpec: An object describing this output.
        """

        delocalize_by = delocalize_by or self._delocalize_by
        if delocalize_by is None:
            raise ValueError("delocalize_by not specified")

        output_spec = _OutputSpec(
            local_path=local_path,
            output_dir=output_dir or self._output_dir,
            output_path=output_path,
            name=name,
            delocalize_by=delocalize_by,
        )

        self._preprocess_output(output_spec)

        self._outputs.append(output_spec)

        return output_spec

    def outputs(self, local_path, *local_paths, output_dir=None, name=None, delocalize_by=None):
        """Define one or more outputs.

        Args:
            local_path (str): The file or directory path within the execution container's file system.
            output_dir (str): Optional destination directory to which the given local_path(s) should be delocalized.
            name (str): Optional name for the output(s).
            delocalize_by (Delocalize): How the path(s) should be delocalized.

        Returns:
            list: A list of _OutputSpec objects that describe these outputs. The list will contain one entry for each
                passed-in path.
        """
        local_paths = [local_path, *local_paths]
        output_specs = []
        for local_path in local_paths:
            output_spec = self.output(
                local_path,
                output_dir=output_dir,
                name=name,
                delocalize_by=delocalize_by)

            output_specs.append(output_spec)

        if len(local_paths) == 1:
            return output_specs[0]
        else:
            return output_specs

    def depends_on(self, upstream_step):
        """Marks this Step as being downstream of another Step in the pipeline, meaning that this Step can only run
        after the upstream_step has completed successfully.

        Args:
            upstream_step (Step): The upstream Step this Step depends on.
        """
        if isinstance(upstream_step, _Step):
            self._upstream_steps.append(upstream_step)
            upstream_step._downstream_steps.append(self)

        elif isinstance(upstream_step, list):
            self._upstream_steps.extend(upstream_step)
            for _upstream_step in upstream_step:
                _upstream_step._downstream_steps.append(self)

        else:
            raise ValueError(f"Unexpected step object type: {type(upstream_step)}")

    def has_upstream_steps(self):
        """Returns True if this Step has upstream Steps that must run before it runs (ie. that it depends on)"""

        return len(self._upstream_steps) > 0

    def __str__(self):
        s = ""
        if self.step_number is not None:
            s += f"step{self.step_number}"
        if self.step_number is not None and self.short_name  is not None:
            s += ": "
        if self.short_name is not None:
            s += self.short_name

        return s

    def __repr__(self):
        return self.__str__()

    def post_to_slack(self, message, channel=None, slack_token=None):
        """Posts the given message to slack. Requires python3 and pip to be installed in the execution environment.

        Args:
            message (str): The message to post.
            channel (str): The Slack channel to post to.
            slack_token (str): Slack auth token.
        """

        args = self._pipeline.parse_args()
        slack_token = slack_token or args.slack_token
        if not slack_token:
            raise ValueError("slack token not provided")
        channel = channel or args.slack_channel
        if not channel:
            raise ValueError("slack channel not specified")

        if not hasattr(self, "_already_installed_slacker"):
            self.command("python3 -m pip install slacker")
            self._already_installed_slacker = True

        self.command(f"""python3 <<EOF
from slacker import Slacker
slack = Slacker("{slack_token}")
response = slack.chat.post_message("{channel}", "{message}", as_user=False, icon_emoji=":bell:", username="step-pipeline-bot")
print(response.raw)
EOF""")

    def switch_gcloud_auth_to_user_account(self, gcloud_credentials_path=None, gcloud_user_account=None,
                                           gcloud_project=None, debug=False):
        """This method adds commands to this Step to switch gcloud auth from the Batch-provided service
        account to the user's personal account.

        This is useful if subsequent commands need to access google buckets that to which the user's personal account
        has access but to which the Batch service account cannot be granted access for whatever reason.

        For this to work, you must first
        1) create a google bucket that only you have access to - for example: gs://weisburd-gcloud-secrets/
        2) on your local machine, make sure you're logged in to gcloud by running
               gcloud auth login
        3) copy your local ~/.config directory (which caches your gcloud auth credentials) to the secrets bucket from step 1
               gsutil -m cp -r ~/.config/  gs://weisburd-gcloud-secrets/
        4) grant your default Batch service-account read access to your secrets bucket so it can download these credentials
           into each docker container.
        5) make sure gcloud & gsutil are installed inside the docker images you use for your Batch jobs
        6) call this method at the beginning of your batch job:

        Example:
              step.switch_gcloud_auth_to_user_account(
                "gs://weisburd-gcloud-secrets", "weisburd@broadinstitute.org", "seqr-project")

        Args:
            gcloud_credentials_path (str): Google bucket path that contains your gcloud auth .config folder.
            gcloud_user_account (str): The user account to activate (ie. "weisburd@broadinstitute.org").
            gcloud_project (str): This will be set as the default gcloud project within the container.
            debug (bool): Whether to add extra "gcloud auth list" commands that are helpful for troubleshooting issues
                with the auth steps.
        """

        args = self._pipeline.parse_args()
        if not gcloud_credentials_path:
            gcloud_credentials_path = args.gcloud_credentials_path
            if not gcloud_credentials_path:
                raise ValueError("gcloud_credentials_path not specified")

        if not gcloud_user_account:
            gcloud_user_account = args.gcloud_user_account
            if not gcloud_user_account:
                raise ValueError("gcloud_user_account not specified")

        if not gcloud_project:
            gcloud_project = args.gcloud_project

        if debug:
            self.command(f"gcloud auth list")
        
        self.command(f"gcloud auth activate-service-account --key-file /gsa-key/key.json")
        self.command(f"gsutil -m cp -r {os.path.join(gcloud_credentials_path, '.config')} /tmp/")
        self.command(f"rm -rf ~/.config")
        self.command(f"mv /tmp/.config ~/")
        self.command(f"gcloud config set account {gcloud_user_account}")
        if gcloud_project:
            self.command(f"gcloud config set project {gcloud_project}")
        
        if debug:
            self.command(f"gcloud auth list")  # print auth list again to check if 'gcloud config set account' succeeded

    @abstractmethod
    def _get_supported_localize_by_choices(self):
        """Returns set of Localize options supported by this pipeline"""
        return set()

    @abstractmethod
    def _get_supported_delocalize_by_choices(self):
        """Returns set of Delocalize options supported by this pipeline"""
        return set()

    @abstractmethod
    def _preprocess_input(self, input_spec):
        """This method is called by step.input(..) immediately when the input is first specified, regardless of whether
        the Step runs or not. It should perform simple checks of the input_spec that are fast and don't require a
        network connection, but that catch simple errors such as incorrect source path syntax.
        _Step subclasses must implement this method.

        Args:
            input_spec (_InputSpec): The input to preprocess.
        """
        if input_spec.localize_by not in self._get_supported_localize_by_choices():
            raise ValueError(f"Unexpected input_spec.localize_by value: {input_spec.localize_by}")

    @abstractmethod
    def _transfer_input(self, input_spec):
        """When a Step isn't skipped and is being transferred to the execution backend, this method is called for
        each input to the Step. It should localize the input into the Step's execution container using the approach
        requested by the user via the localize_by parameter.

        Args:
            input_spec (_InputSpec): The input to localize.
        """
        if input_spec.localize_by not in self._get_supported_localize_by_choices():
            raise ValueError(f"Unexpected input_spec.localize_by value: {input_spec.localize_by}")

    @abstractmethod
    def _preprocess_output(self, output_spec):
        """This method is called by step.output(..) immediately when the output is first specified, regardless of
        whether the Step runs or not. It should perform simple checks of the output_spec that are fast and don't
        require a network connection, but that catch simple errors such as incorrect output path syntax.
        _Step subclasses must implement this method.

        Args:
            output_spec (_OutputSpec): The output to preprocess.
        """
        if output_spec.delocalize_by not in self._get_supported_delocalize_by_choices():
            raise ValueError(f"Unexpected output_spec.delocalize_by value: {output_spec.delocalize_by}")

    @abstractmethod
    def _transfer_output(self, output_spec):
        """When a Step isn't skipped and is being transferred to the execution backend, this method will be called for
        each output of the Step. It should delocalize the output from the Step's execution container to the requested
        destination path using the approach requested by the user via the delocalize_by parameter.

        Args:
            output_spec (_OutputSpec): The output to delocalize.
        """
        if output_spec.delocalize_by not in self._get_supported_delocalize_by_choices():
            raise ValueError(f"Unexpected output_spec.delocalize_by value: {output_spec.delocalize_by}")

    def record_memory_cpu_and_disk_usage(self, output_dir, time_interval=5, export_json=True, export_graphs=False, install_glances=True):
        """Add commands that run the 'glances' python tool to record memory, cpu, disk usage and other profiling stats
        in the background at regular intervals.

        Args:
            output_dir (str): Profiling data will be written to this directory.
            time_interval (int): How frequently to update the profiling data files.
            export_json (bool): Whether to export a glances.json file to output_dir.
            export_graphs (bool): Whether to export .svg graphs.
            install_glances (bool): If True, a command will be added to first install the 'glances' python library
                inside the execution container.
        """
        if install_glances and not hasattr(self, "_already_installed_glances"):
            self.command("python3 -m pip install --upgrade glances")
            self._already_installed_glances = True

        if export_json:
            json_path = os.path.join(output_dir, "glances.json")
            self.command(f"""python3 -m glances -q --export json --export-json-file {json_path} -t {time_interval} &""")

        if export_graphs:
            self.command(f"""python3 -m glances -q --export graph --export-graph-path {output_dir} --config <(echo '
[graph]
generate_every={time_interval}
width=1400
height=1000
style=DarktStyle
') &
""")
