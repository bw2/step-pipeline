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

For concreteness, the docs below are written with Hail Batch as the execution engine. However, this library is designed
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
        - new_step(self, name, reuse_job=False, cpu=None, memory=None, disk=None)
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
import os
import re

from .utils import are_outputs_up_to_date


class ExecutionEngine:
    HAIL_BATCH = "hail_batch"
    #CROMWELL = "cromwell"
    #DSUB = "dsub"


class LocalizationStrategy(Enum):
    """This class defines localization strategies. The 2-tuple contains a label and a subdirectory where to put files"""
    COPY = ("copy", "local_copy")

    """GSUTIL_COPY requires gsutil to be installed inside the execution container"""
    GSUTIL_COPY = ("gsutil_copy", "local_copy")

    """HAIL_HADOOP_COPY requires python3 and hail to be installed inside the execution container"""
    HAIL_HADOOP_COPY = ("hail_hadoop_copy", "local_copy") 
    HAIL_BATCH_GCSFUSE = ("hail_batch_gcsfuse", "gcsfuse")
    HAIL_BATCH_GCSFUSE_VIA_TEMP_BUCKET = ("hail_batch_gcsfuse_via_temp_bucket", "gcsfuse")

    def __init__(self, label, subdir="local_copy"):
        self._label = label
        self._subdir = subdir

    def __str__(self):
        return self._label

    def __repr__(self):
        return self._label

    def get_subdir_name(self):
        return self._subdir


class DelocalizationStrategy(Enum):
    """This class defines delocalization strategies. The value represents a name for each strategy"""

    """HAIL_HADOOP_COPY requires python3 and hail to be installed inside the execution container"""
    COPY = "copy"

    """GSUTIL_COPY requires gsutil to be installed inside the execution container"""
    GSUTIL_COPY = "gsutil_copy"

    """HAIL_HADOOP_COPY requires python3 and hail to be installed inside the execution container"""
    HAIL_HADOOP_COPY = "hail_hadoop_copy"

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value


class _InputSpec:
    """An InputSpec stores metadata about an input file to a Step"""

    def __init__(self,
        source_path: str,
        name: str = None,
        localization_strategy: str = None,
        localization_root_dir: str = None,
    ):
        self._source_path = source_path
        self._localization_strategy = localization_strategy

        self._source_bucket = None
        if source_path.startswith("gs://"):
            self._source_path_without_protocol = re.sub("^gs://", "", source_path)
            self._source_bucket = self._source_path_without_protocol.split("/")[0]
        elif source_path.startswith("http://") or source_path.startswith("https://"):
            self._source_path_without_protocol = re.sub("^http[s]?://", "", source_path).split("?")[0]
        else:
            self._source_path_without_protocol = source_path

        self._source_dir = os.path.dirname(self._source_path_without_protocol)
        self._filename = os.path.basename(self._source_path_without_protocol).replace("*", "_._")

        self._name = name or self._filename

        subdir = localization_strategy.get_subdir_name()
        output_dir = os.path.join(localization_root_dir, subdir, self.get_source_dir().strip("/"))
        output_dir = output_dir.replace("*", "___")

        self._local_dir = output_dir
        self._local_path = os.path.join(output_dir, self.get_filename())

    def get_source_path(self):
        return self._source_path

    def get_source_bucket(self):
        return self._source_bucket

    def get_source_path_without_protocol(self):
        return self._source_path_without_protocol

    def get_source_dir(self):
        return self._source_dir

    def get_filename(self):
        return self._filename

    def get_input_name(self):
        return self._name

    def get_local_path(self):
        return self._local_path

    def get_local_dir(self):
        return self._local_dir

    def get_localization_strategy(self):
        return self._localization_strategy


class _OutputSpec:
    """An OutputSpec represents a description of an output file from a Step"""

    def __init__(self,
        local_path: str,
        output_dir: str = None,
        output_path: str = None,
        name: str = None,
        delocalization_strategy: str = None):

        self._local_path = local_path
        self._local_dir = os.path.dirname(local_path)
        self._name = name
        self._delocalization_strategy = delocalization_strategy

        if output_path:
            self._output_filename = os.path.basename(output_path)
        elif "*" not in local_path:
            self._output_filename = os.path.basename(local_path)
        else:
            self._output_filename = None

        if output_dir:
            self._output_dir = output_dir
            if output_path:
                if os.path.isabs(output_path) or output_path.startswith("gs://"):
                    raise ValueError(f"output_dir ({output_dir}) specified even though output_path provided as "
                                 f"an absolution path ({output_path})")
                self._output_path = os.path.join(output_dir, output_path)
            elif self._output_filename:
                self._output_path = os.path.join(output_dir, self._output_filename)
            else:
                self._output_path = output_dir

        elif output_path:
            self._output_path = output_path
            self._output_dir = os.path.dirname(self._output_path)
        else:
            raise ValueError("Neither output_dir nor output_path were specified.")

        if "*" in self._output_path:
            raise ValueError(f"output path ({output_path}) cannot contain wildcards (*)")

    def get_output_path(self):
        return self._output_path

    def get_output_dir(self):
        return self._output_dir

    def get_output_filename(self):
        return self._output_filename

    def get_output_name(self):
        return self._name

    def get_local_path(self):
        return self._local_path

    def get_local_dir(self):
        return self._local_dir

    def get_delocalization_strategy(self):
        return self._delocalization_strategy


class _Pipeline(ABC):
    """The _Pipeline object is returned to the user and serves as the gateway for accessing all other functionality in 
    this library. It is the parent class for execution-engine-specific classes such as _BatchPipeline.
    _Pipeline itself is generic in the sense that it doesn't have code specific to a particular execution engine.
    
    Its public methods are factory methods for creating Steps.
    It also has some private methods that implement the generic aspects of traversing the DAG and transferring
    all steps to a specific execution engine.
    """

    def __init__(self, name=None, config_arg_parser=None):
        """Constructor.

        Args:
            name (str): A name for the pipeline.
            config_arg_parser (configargparse.ArgumentParser): For defining pipeline command-line args.
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
        args = p.parse_args()
        """
        return self._config_arg_parser

    def parse_args(self):
        args, _ = self._config_arg_parser.parse_known_args(ignore_help_args=True)
        return args

    @abstractmethod
    def new_step(self, short_name, step_number=None):
        """Creates a new step that depends on this step"""

    @abstractmethod
    def run(self):
        """Submits a pipeline to an execution engine such as Hail Batch. In performs any initialization of the specific
        execution environment and then calling the generic self._transfer_all_steps(..) method.
        """

    @abstractmethod
    def _get_localization_root_dir(self, localization_strategy):
        """Returns the top level directory where files should be localized. For example /data/mounted_disk/"""

    def __enter__(self):
        """Enables code like:

        with step_pipeline() as sp:
            sp.new_step(..)
            ..
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Runs at the completion of a 'with' block .. """

        self.run()

    def _transfer_all_steps(self):
        """Independent of a specific execution engine"""

        args = self.parse_args()

        steps_to_run_next = [s for s in self._all_steps if not s.has_upstream_steps()]
        num_steps_transferred = 0
        while steps_to_run_next:
            print("Next steps: ", steps_to_run_next)
            for step in steps_to_run_next:
                if not step._commands:
                    print(f"Skipping {step}. No commands found.")
                    continue

                # decide whether to skip this step
                skip_requested = any(args.get(skip_arg_name) for skip_arg_name in step._skip_this_step_arg_names)

                is_being_forced = args.force or any(
                    args.get(force_arg_name) for force_arg_name in step._force_this_step_arg_names)
                all_upstream_steps_skipped = all(s._is_being_skipped for s in step._upstream_steps)
                no_need_to_run_step = not is_being_forced and are_outputs_up_to_date(step) and all_upstream_steps_skipped
                step._is_being_skipped = skip_requested or no_need_to_run_step

                if step._is_being_skipped:
                    print(f"Skipping {step}")
                else:
                    print(f"Running {step}")
                    step._transfer_step()
                    num_steps_transferred += 1

            # next, process all steps that depend on the previously-completed steps
            steps_to_run_next = [downstream_step for step in steps_to_run_next for downstream_step in step._downstream_steps]

        return num_steps_transferred


class _Step(ABC):
    """Represents a set of commands or sub-steps which together produce some output file(s), and which can be
    skipped if the output files already exist (and are newer than any input files unless a --force arg is used).
    A Step's input and output files must exist in storage that persists across Steps - like Google Cloud Storage.

    A Step corresponds to a single Job, but in some cases one Batch Job may be reused for multiple steps
    (for example, step 1 creates a VCF and step 2 tabixes it).
    """

    _USED_ARG_SUFFIXES = set()
    _USED_STEP_NUMBERS = set()

    def __init__(
            self,
            pipeline: _Pipeline,
            short_name: str,
            arg_suffix: str = None,
            step_number: int = None,
            output_dir: str = None,
            default_localization_strategy: str = None,
            default_delocalization_strategy: str = None,
    ):
        """_Step constructor

        :param pipeline: _Pipeline object
        :param short_name: a short name for this step
        :param arg_suffix: if specified, --skip-{arg_suffix} and --force-{arg_suffix} command-line args will be created.
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

        if arg_suffix and arg_suffix not in _Step._USED_ARG_SUFFIXES:
            argument_parser.add_argument(
                f"--force-{arg_suffix}",
                help=f"Force execution of the '{short_name}' step.",
                action="store_true",
            )
            self._force_this_step_arg_names.append(f"force_{arg_suffix}")
            argument_parser.add_argument(
                f"--skip-{arg_suffix}",
                help=f"Skip the '{short_name}' step even if --force is used.",
                action="store_true",
            )
            self._skip_this_step_arg_names.append(f"skip_{arg_suffix}")
            _Step._USED_ARG_SUFFIXES.add(arg_suffix)

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

    def short_name(self, short_name):
        self.short_name = short_name

    def output_dir(self, output_dir: str):
        self._output_dir = output_dir

    def command(self, command):
        """Adds a command"""
        self._commands.append(command)

    def input_glob(self, glob, name=None, localization_strategy=None):
        return self.input(glob, name=name, localization_strategy=localization_strategy)

    def input(self,
              source_path: str,
              name: str = None,
              localization_strategy: str = None):
        """Specifies an input file or glob.

        :param source_path:
        :param name:
        :param localization_strategy:
        :return: input spec dictionary with these keys:

        """
        localization_strategy = localization_strategy or self._default_localization_strategy

        # validate inputs
        if not source_path.startswith("gs://") and localization_strategy in (
                LocalizationStrategy.GSUTIL_COPY,
                LocalizationStrategy.HAIL_BATCH_GCSFUSE,
                LocalizationStrategy.HAIL_BATCH_GCSFUSE_VIA_TEMP_BUCKET):
            raise ValueError(f"source_path '{source_path}' doesn't start with gs://")

        input_spec = _InputSpec(
            source_path=source_path,
            name=name,
            localization_strategy=localization_strategy,
            localization_root_dir=self._pipeline._get_localization_root_dir(localization_strategy),
        )

        self._preprocess_input(input_spec)
        self._inputs.append(input_spec)

        return input_spec

    def use_the_same_inputs_as(self, other_step, localization_strategy=None):
        localization_strategy = localization_strategy or self._default_localization_strategy

        input_specs = []
        for other_step_input_spec in other_step._inputs:
            input_spec = self.input(
                source_path=other_step_input_spec.get_source_path(),
                name=other_step_input_spec.get_output_name(),
                localization_strategy=localization_strategy or other_step_input_spec.get_localization_strategy(),
            )
            input_specs.append(input_spec)
        return input_specs

    def use_previous_step_outputs_as_inputs(self, previous_step, localization_strategy=None):
        self.depends_on(previous_step)

        localization_strategy = localization_strategy or self._default_localization_strategy

        input_specs = []
        for output_spec in previous_step._outputs:
            input_spec = self.input(
                source_path=output_spec.get_output_path(),
                name=output_spec.get_output_name(),
                localization_strategy=localization_strategy,
            )
            input_specs.append(input_spec)

        return input_specs

    def output_dir(self, path):
        """If output is a relative path, it will be relative to this dir"""
        self._output_dir = path

    def output(
            self,
            local_path,
            output_path: str = None,
            output_dir: str = None,
            name: str = None,
            delocalization_strategy: str = None
    ):
        """Specify a pipeline output file.

        Args:
            local_path:
            output_path:
            output_dir:
            name:
            delocalization_strategy:

        Returns:
            output spec
        """
        delocalization_strategy = delocalization_strategy or self._default_delocalization_strategy
        if delocalization_strategy is None:
            raise ValueError("delocalization_strategy not specified")

        output_spec = _OutputSpec(
            local_path=local_path,
            output_dir=output_dir or self._output_dir,
            output_path=output_path,
            name=name,
            delocalization_strategy=delocalization_strategy,
        )

        self._preprocess_output(output_spec)

        self._outputs.append(output_spec)

        return output_spec

    def depends_on(self, upstream_step):
        if isinstance(upstream_step, _Step):
            self._upstream_steps.append(upstream_step)
            upstream_step._downstream_steps.append(self)

        elif isinstance(upstream_step, list):
            self._upstream_steps.extend(upstream_step)
            for _upstream_step in upstream_step:
                _upstream_step._downstream_steps.append(self)

        else:
            raise ValueError(f"Unexpected step object type: {type(upstream_step)}")

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

        Args:
            gcloud_credentials_path (str): google bucket path that contains your .config folder. This value can be specified via command line or a config file instead.
            gcloud_user_account (str): user account to activate. This value can be specified via command line or a config file instead.
            gcloud_project (str): set this as the default gcloud project. This value can be specified via command line or a config file instead.
            debug (bool): Whether to add extra commands that are helpful for troubleshooting issues with the auth steps.
        """

        args = self.parse_args()
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

    def parse_args(self):
        return self._pipeline.parse_args()

    def _get_supported_localization_strategies(self):
        return {
            LocalizationStrategy.HAIL_HADOOP_COPY,
        }

    def _get_supported_delocalization_strategies(self):
        return {
            DelocalizationStrategy.HAIL_HADOOP_COPY,
        }

    def _add_commands_for_hail_hadoop_copy(self, source_path, output_dir):
        #if not hasattr(self, "_already_installed_hail"):
        #    self.command("python3 -m pip install hail")
        #self._already_installed_hail = True

        #self.command(f"mkdir -p {output_dir}")
        self.command(f"""python3 <<EOF
import hail as hl
hl.hadoop_copy("{source_path}", "{output_dir}")
EOF""")

    def _preprocess_input(self, input_spec):
        """This method is called by step.input(..) at input definition time, regardless of whether the step runs or not.
        It's meant to perform validation and initialization steps that are fast and don't require a network connection.
        _Step subclasses can override this method to perform execution-engine-specific pre-processing of inputs.
        """
        localization_strategy = input_spec.get_localization_strategy()
        if localization_strategy not in self._get_supported_localization_strategies():
            raise ValueError(f"Unsupported localization strategy: {localization_strategy}")

        if localization_strategy == LocalizationStrategy.HAIL_HADOOP_COPY:
            self._add_commands_for_hail_hadoop_copy(input_spec.get_source_path(), input_spec.get_local_dir())

    def _transfer_input(self, input_spec):
        """This method is called when the pipeline is being transferred to the execution engine, and only if the Step
        is not being skipped."""
        localization_strategy = input_spec.get_localization_strategy()
        if localization_strategy not in self._get_supported_localization_strategies():
            raise ValueError(f"Unsupported localization strategy: {localization_strategy}")

    def _preprocess_output(self, output_spec):
        delocalization_strategy = output_spec.get_delocalization_strategy()
        if delocalization_strategy not in self._get_supported_delocalization_strategies():
            raise ValueError(f"Unsupported delocalization strategy: {delocalization_strategy}")

        if delocalization_strategy == DelocalizationStrategy.HAIL_HADOOP_COPY:
            self._add_commands_for_hail_hadoop_copy(output_spec.get_local_path(), output_spec.get_output_dir())

    def _transfer_output(self, output_spec):
        delocalization_strategy = output_spec.get_delocalization_strategy()
        if delocalization_strategy not in self._get_supported_delocalization_strategies():
            raise ValueError(f"Unsupported delocalization strategy: {delocalization_strategy}")

    def _add_profiling_commands(self):
        raise ValueError("Not yet implemented")


