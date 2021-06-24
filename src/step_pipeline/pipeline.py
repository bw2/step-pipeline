"""
This module implements a "Step" abstraction which makes it easier to write pipelines with less repetitive code.
It handles checking which steps can be skipped because they already ran, etc.

To make things less abstract, the docs below assume Hail Batch is the execution engine. However, this module
is designed for other execution engines to be added over time - such as "local", "cromwell", "SGE", "dsub", etc.

A single Step contains a set of commands which together produce some output file(s), and which can be
skipped if the output files already exist (and are newer than any input files unless a --force arg is used).

Besides doing these checks on inputs/outputs, a Step will automatically define argparse args for skipping or forcing
the step's execution, localize/delocalize the input/output files, and possibly collect timing and profiling info
on memory & disk use (such as starting a background command to record free memory and disk use every 10 seconds,
localizing/delocalizing a .tsv file that accumulates memory and other stats for all steps)

By default, a Step corresponds on a single Batch Job, but in some cases the same Batch Job may be reused for
multiple steps (for example, step 1 creates a VCF and step 2 tabixes it), or

Specifically, this wrapper will allow code that looks like:

with batch_wrapper.batch_pipeline(desc="merge vcfs and compute per-chrom counts") as bp:
     bp.add_argument("--my-flag", action="store_true", help="toggle something")
     args = bp.parse_known_args() # if needed, parse argparse args that have been defined so far

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

# here the wrapper will parse commmand-line args, decide which steps to skip, pass the DAG to Batch, and then call batch.run


_Step:
    - not specific to any execution engine
    - data:
        - list of inputs
        - list of outputs
        - list of commands and _Steps contained within this _Step
        - output dir
        - list of upstream steps
        - list of downstream steps
        - name
    - methods:
        - next_step(self, name, reuse_job=False, cpu=None, memory=None, disk=None)
        - command(self, command)
        - input(self, path, name=None, use_gcsfuse=False)
        - input_glob(self, glob, name=None, use_gcsfuse=False)
        - output(self, path, name=None)
        - output_dir(self, path)
        - depends_on(self, step, reuse_job=False)
        - has_upstream_steps(self)


_Pipeline
    - parent class for execution-engine-specific classes
    - methods:
        - _transfer_step(self, step: _Step, execution_context):        - not specific to any execution engine

_BatchPipeline

utils:
    are_any_inputs_missing(step: _Step) -> bool
    are_outputs_up_to_date(step: _Step) -> bool
"""


from abc import ABC, abstractmethod
from enum import Enum

import configargparse
import contextlib
import os
import re

from .utils import are_outputs_up_to_date


class ExecutionEngine:
    HAIL_BATCH = "hail_batch"
    CROMWELL = "cromwell"
    DSUB = "dsub"


class LocalizationStrategy(Enum):
    COPY = ("copy", "localized/")

    GSUTIL_COPY = ("gsutil_copy", "localized/")  # requires gsutil to already be installed

    HAIL_HADOOP_COPY = ("hail_hadoop_copy", "localized/")  # requires python3 and hail to already be installed
    HAIL_BATCH_GCSFUSE = ("hail_batch_gcsfuse", "gcsfuse_mounts/")
    HAIL_BATCH_GCSFUSE_VIA_TEMP_BUCKET = ("hail_batch_gcsfuse_via_temp_bucket", "gcsfuse_mounts/")

    def __init__(self, name, subdir="localized/"):
        self._name = name
        self._subdir = subdir

    def __str__(self):
        return self._name

    def __repr__(self):
        return self._name

    def get_subdir_name(self):
        return self._subdir


class DelocalizationStrategy(Enum):
    COPY = "copy"

    GSUTIL_COPY = "gsutil_copy"  # requires gsutil to already be installed

    HAIL_HADOOP_COPY = "hail_hadoop_copy"  # requires python3 and hail to already be installed

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value


class _Pipeline(ABC):
    """The parent class for _Pipeline classes such as _BatchPipeline that implement pipeline operations for a specific
    execution engine. The parent class is general, and doesn't have execution-engine-specific code.
    The _Pipeline object is returned to the user and serves as the gateway for accessing this module's functionality.
    Its public methods are factories for creating steps.
    It also has some private methods that implement the generic aspects of traversing the DAG and transferring
    all steps to a specific execution engine.
    """

    def __init__(self, argument_parser, name=None):
        self._argument_parser = argument_parser
        self.name = name
        self._all_steps = []

        argument_parser.add_argument("-v", "--verbose", action='count', default=0, help="Print more info")
        argument_parser.add_argument("-c", "--config-file", help="YAML config file path", is_config_file_arg=True)
        argument_parser.add_argument("--dry-run", action="store_true", help="Don't run commands, just print them.")
        argument_parser.add_argument("-f", "--force", action="store_true", help="Force execution of all steps.")

        grp = argument_parser.add_argument_group("notifications")
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
        args = p.parse_args()
        """
        return self._argument_parser

    @abstractmethod
    def new_step(self, short_name, step_number=None):
        """Creates a new step that depends on this step"""

    @abstractmethod
    def run(self):
        """Submits a pipeline to an execution engine such as Batch, Terra, SGE, etc. by setting up the
        execution environment and then calling the generic self._transfer_all_steps(..) method.
        """

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._argument_parser.parse_args()

        # execute pipeline
        print(f"Starting {name or ''} pipeline:")
        self.run()

    def _get_args(self):
        args, _ = self._argument_parser.parse_known_args(ignore_help_args=True)
        return args

    def _transfer_all_steps(self):
        """Independent of a specific execution engine"""

        args = self._get_args()

        steps_to_run_next = [s for s in self._all_steps if not s.has_upstream_steps()]
        print("Steps to run next: ", steps_to_run_next)
        num_steps_transferred = 0
        while steps_to_run_next:
            for step in steps_to_run_next:
                if not step._commands:
                    print("Skipping step: ", step, " because it doesn't contain any commands")
                    continue

                skip_requested = any(args.get(skip_arg_name) for skip_arg_name in step._skip_this_step_arg_names)
                step_needs_to_run = (
                        args.force
                        or any(args.get(force_arg_name) for force_arg_name in step._force_this_step_arg_names)
                        or not are_outputs_up_to_date(step)
                        or any(not s._is_being_skipped for s in step._upstream_steps))
                step._is_being_skipped = skip_requested or not step_needs_to_run

                if step._is_being_skipped:
                    print(f"Skipping step: {step}")
                else:
                    print(f"Running step {step}")
                    step._transfer_input()
                    step._transfer_step()
                    step._transfer_output()
                    num_steps_transferred += 1

            # next, process all steps that depend on the previously-completed steps
            steps_to_run_next = [s for step in steps_to_run_next for s in step._downstream_steps]

        return num_steps_transferred


class _Step(ABC):
    """Represents a set of commands or sub-steps which together produce some output file(s), and which can be
    skipped if the output files already exist (and are newer than any input files unless a --force arg is used).
    A Step's input and output files must exist in storage that persists across Steps - like Google Cloud Storage.

    A Step corresponds to a single Job, but in some cases one Batch Job may be reused for multiple steps
    (for example, step 1 creates a VCF and step 2 tabixes it).
    """

    _USED_ARG_NAMES = set()
    _USED_STEP_NUMBERS = set()

    def __init__(
            self,
            pipeline: _Pipeline,
            short_name: str,
            arg_name: str = None,
            step_number: int = None,
            output_dir: str = None,
            default_localization_strategy: str = None,
            default_delocalization_strategy: str = None,
    ):
        """_Step constructor

        :param pipeline: _Pipeline object
        :param short_name: a short name for this step
        :param arg_name: if specified, --skip-{arg_name} and --force-{arg_name} command-line args will be created.
        :param step_number: if specified, --skip-step{step_number} and --force-step{step_number} command-line args will be created.
        """
        self.short_name = short_name
        self._pipeline = pipeline

        self._output_dir = output_dir
        self._default_localization_strategy = default_localization_strategy
        self._default_delocalization_strategy = default_delocalization_strategy

        self._inputs = []
        self._outputs = []

        self._commands = []   # used for BashJobs
        self._calls = []  # use for PythonJobs

        self._upstream_steps = []  # this step depends on these steps
        self._downstream_steps = []  # steps that depend on this step
        #self._substeps = []  # steps that are contained within this step. If this is not empty

        self._is_being_skipped = False

        self._force_this_step_arg_names = []
        self._skip_this_step_arg_names = []

        # define command line args for skipping or forcing execution of this step
        argument_parser = pipeline.get_config_arg_parser()

        if arg_name and arg_name not in _Step._USED_ARG_NAMES:
            argument_parser.add_argument(
                f"--force-{arg_name}",
                help=f"Force execution of the '{short_name}' step.",
                action="store_true",
            )
            self._force_this_step_arg_names.append(f"force_{arg_name}")
            argument_parser.add_argument(
                f"--skip-{arg_name}",
                help=f"Skip the '{short_name}' step even if --force is used.",
                action="store_true",
            )
            self._skip_this_step_arg_names.append(f"skip_{arg_name}")
            _Step._USED_ARG_NAMES.add(arg_name)

        if step_number is not None and step_number not in _Step._USED_STEP_NUMBERS:
            try:
                step_number = int(step_number)
            except Exception as e:
                raise ValueError(f"Invalid step_number arg: {step_number}. {e}")

            argument_parser.add_argument(
                f"--force-step{step_number}",
                help=f"Force execution of the '{short_name}' step.",
                action="store_true",
            )
            self._force_this_step_arg_names.append(f"force_step{step_number}")
            argument_parser.add_argument(
                f"--skip-step{step_number}",
                help=f"Skip the '{short_name}' step even if --force is used.",
                action="store_true",
            )
            self._skip_this_step_arg_names.append(f"skip_step{step_number}")
            _Step._USED_STEP_NUMBERS.add(step_number)

    def command(self, command):
        """Adds a command"""
        self._commands.append(command)

    def input_glob(self, glob, destination_root_dir=None, name=None, localization_strategy=None):
        self.input(glob, name=name, destination_root_dir=destination_root_dir, localization_strategy=localization_strategy)

    def input(self,
              source_path: str,
              destination_root_dir: str = "/",
              name: str = None,
              localization_strategy: str = None
              ):
        """Specifies an input file or glob.

        :param source_path:
        :param name:
        :param destination_root_dir:
        :param localization_strategy:
        :return: input spec dictionary with these keys:

        """
        localization_strategy = localization_strategy or self._default_localization_strategy or LocalizationStrategy.COPY

        # validate inputs
        if localization_strategy in (
                LocalizationStrategy.GSUTIL_COPY,
                LocalizationStrategy.HAIL_BATCH_GCSFUSE,
                LocalizationStrategy.HAIL_BATCH_GCSFUSE_VIA_TEMP_BUCKET) and not source_path.startswith("gs://"):
            raise ValueError(f"source_path '{source_path}' doesn't start with gs://")

        input_spec = {
            "source_path": source_path,
            "destination_root_dir": destination_root_dir,
            "localization_strategy": localization_strategy,
        }

        if source_path.startswith("gs://"):
            input_spec["source_path_without_protocol"] = re.sub("^gs://", "", source_path)
            input_spec["source_bucket"] = input_spec["source_path_without_protocol"].split("/")[0]
        elif source_path.startswith("http://") or source_path.startswith("https://"):
            input_spec["source_path_without_protocol"] = re.sub("^http[s]?://", "", source_path).split("?")[0]
        else:
            input_spec["source_path_without_protocol"] = source_path

        input_spec["filename"] = os.path.basename(input_spec["source_path_without_protocol"]).replace("*", "_._")
        input_spec["source_dir"] = os.path.dirname(input_spec["source_path_without_protocol"])
        subdir = localization_strategy.get_subdir_name()
        destination_dir = os.path.join(destination_root_dir, subdir, input_spec["source_dir"].strip("/"))
        destination_dir = destination_dir.replace("*", "_._")

        input_spec["local_dir"] = destination_dir
        input_spec["local_path"] = os.path.join(destination_dir, input_spec["filename"])

        if not name:
            name = input_spec["filename"]
        input_spec["name"] = name

        self._preprocess_input(input_spec)
        self._inputs.append(input_spec)

        return input_spec

    def use_the_same_inputs_as(self, other_step, localization_strategy=None):
        input_specs = []
        for input in other_step._inputs:
            input_spec = self.input(
                source_path=input["path"],
                name=input["name"],
                destination_root_dir=input["destination_root_dir"],
                localization_strategy=localization_strategy if localization_strategy is not None else input["localization_strategy"],
            )
            input_specs.append(input_spec)
        return input_specs

    def use_previous_step_outputs_as_inputs(self, previous_step, destination_root_dir=None, localization_strategy=None):
        self.depends_on(previous_step)

        localization_strategy = localization_strategy or self._default_localization_strategy or LocalizationStrategy.COPY

        input_specs = []
        for output in previous_step._outputs:
            input_spec = self.input(
                source_path=output["path"],
                name=output["name"],
                destination_root_dir=destination_root_dir,
                localization_strategy=localization_strategy,
            )
            input_specs.append(input_spec)

        return input_specs

    def output(self, local_path, destination_path=None, destination_dir=None, name=None, delocalization_strategy=DelocalizationStrategy.COPY):
        if not destination_dir and not destination_path and not self._output_dir:
            raise ValueError("No output destination specified")

        if destination_path and destination_dir and (os.path.isabs(destination_path) or destination_path.startswith("gs://")):
            raise ValueError(f"destination_dir ({destination_dir}) specified even though destination_path provided as "
                             f"an absolution path ({destination_path})")

        if not destination_dir and self._output_dir:
            destination_dir = self._output_dir

        destination_filename = os.path.basename(destination_path) if destination_path else os.path.basename(local_path)

        if destination_dir and (not destination_path or not os.path.isabs(destination_path)):
            destination_path = os.path.join(destination_dir, destination_filename)

        if "*" in destination_path:
            raise ValueError(f"destination path ({destination_path}) cannot contain wildcards (*)")

        delocalization_strategy = delocalization_strategy or self._default_delocalization_strategy or DelocalizationStrategy.COPY
        output_spec = {
            "name": name or destination_filename,
            "destination_filename": destination_filename,
            "local_path": local_path,
            "destination_dir": os.path.dirname(destination_path),
            "destination_path": destination_path,
            "delocalization_strategy": delocalization_strategy,
        }

        self._preprocess_output(output_spec)

        self._outputs.append(output_spec)

        return output_spec

    def output_dir(self, path):
        """If output is a relative path, it will be relative to this dir"""
        self._output_dir = path

    def depends_on(self, step):
        if isinstance(step, _Step):
            self._upstream_steps.append(step)
            self._downstream_steps.append(self)

        elif isinstance(step, list):
            self._upstream_steps.extend(step)
            for upstream_step in self._upstream_steps:
                upstream_step._downstream_steps.append(self)

    def has_upstream_steps(self) -> bool:
        return len(self._upstream_steps) > 0

    def delete_paths_on_completion(self, paths, only_on_success=False):
        raise Exception("Not yet implemented")

    def __str__(self):
        return self.short_name

    def __repr__(self):
        return self.short_name

    def post_to_slack(self, message, channel=None, slack_token=None):
        """
        Adds a command to post to slack. Requires python3 and pip to be installed in the execution environment.
        """

        argument_parser = self._pipeline.get_config_arg_parser()
        args, _ = argument_parser.parse_known_args(ignore_help_args=True)
        slack_token = slack_token or args.slack_token
        if not slack_token:
            raise ValueError("slack token not provided")
        channel = channel or args.slack_channel
        if not channel:
            raise ValueError("slack channel not specified")

        self.command("python3 -m pip install slacker")
        self.command(f"""python3 <<EOF
from slacker import Slacker
slack = Slacker("{slack_token}")
response = slack.chat.post_message("{channel}", "{message}", as_user=False, icon_emoji=":bell:", username="step-pipeline-bot")
print(response.raw)
EOF""")

    def switch_gcloud_auth_to_user_account(
            self,
            gcloud_credentials_path: str = None,
            gcloud_user_account: str = None,
            gcloud_project: str = None,
            debug: bool = False,
    ):
        """This method adds commands to switch gcloud auth from the Batch-provided service
        account to your user account.

        This is useful is subsequent commands need to access google buckets that your personal account has access to
        without having to first grant access to the Batch service account

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
              switch_gcloud_auth_to_user_account(
                batch_job,
                "gs://weisburd-gcloud-secrets",
                "weisburd@broadinstitute.org",
                "seqr-project")

        :param gcloud_credentials_path: google bucket path that contains your .config folder
        :param gcloud_user_account: user account to activate
        :param gcloud_project: (optional) set this as the default gcloud project
        :return:
        """

        args = self._get_args()
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
            self.command(f"gcloud auth list")  # print auth list again to show that 'gcloud config set account' succeeded.

    def _get_args(self):
        return self._pipeline.get_args()

    def _get_supported_localization_strategies(self):
        return {
            LocalizationStrategy.HAIL_HADOOP_COPY,
        }

    def _get_supported_delocalization_strategies(self):
        return {
            DelocalizationStrategy.HAIL_HADOOP_COPY,
        }

    def _add_commands_for_hail_hadoop_copy(self, source_path, destination_dir):
        self.command("python3 -m pip install hail")
        self.command(f"mkdir -p {destination_dir}")
        self.command(f"""python3 <<EOF
import hail as hl
hl.hadoop_copy("{source_path}", "{destination_dir}/")
EOF""")

    def _preprocess_input(self, input_spec):
        localization_strategy = input_spec["localization_strategy"]
        if localization_strategy not in self._get_supported_localization_strategies():
            raise ValueError(f"Unsupported localization strategy: {localization_strategy}")

        if localization_strategy == LocalizationStrategy.HAIL_HADOOP_COPY:
            self._add_commands_for_hail_hadoop_copy(input_spec["source_path"], input_spec["local_dir"])

    def _transfer_input(self, input_spec):
        localization_strategy = input_spec["localization_strategy"]
        if localization_strategy not in self._get_supported_localization_strategies():
            raise ValueError(f"Unsupported localization strategy: {localization_strategy}")

    def _preprocess_output(self, output_spec):
        delocalization_strategy = output_spec["delocalization_strategy"]
        if delocalization_strategy not in self._get_supported_delocalization_strategies():
            raise ValueError(f"Unsupported delocalization strategy: {delocalization_strategy}")

        if delocalization_strategy == DelocalizationStrategy.HAIL_HADOOP_COPY:
            self._add_commands_for_hail_hadoop_copy(output_spec["local_path"], output_spec["destination_dir"])

    def _transfer_output(self, output_spec):
        delocalization_strategy = output_spec["delocalization_strategy"]
        if delocalization_strategy not in self._get_supported_delocalization_strategies():
            raise ValueError(f"Unsupported delocalization strategy: {delocalization_strategy}")

    def _add_profiling_commands(self):
        raise ValueError("Not yet implemented")


def step_pipeline(
        name=None,
        execution_engine=ExecutionEngine.HAIL_BATCH,
        config_file="~/.step_pipeline",
):
    """Creates and starts a pipeline.

    :param name: (optional) pipeline name that will appear
    :param execution_engine:
    :param config_file:

    Usage:
        with step_pipeline(args) as sp:
            ... step definitions ...
    """

    # define argparser
    argument_parser = configargparse.ArgumentParser(
        add_config_file_help=True,
        add_env_var_help=True,
        formatter_class=configargparse.HelpFormatter,
        default_config_files=[config_file],
        ignore_unknown_config_file_keys=True,
        config_file_parser_class=configargparse.YAMLConfigFileParser,
    )

    # create and yield the pipeline
    if execution_engine == ExecutionEngine.HAIL_BATCH:
        from batch import _BatchPipeline
        pipeline = _BatchPipeline(argument_parser, name=name)
    else:
        raise ValueError(f"Unsupported execution_engine: '{execution_engine}'")

    return pipeline
