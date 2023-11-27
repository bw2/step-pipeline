import collections
from abc import ABC, abstractmethod

import configargparse
import os
import random
import re
import sys

from step_pipeline.utils import _file_stat__cached
from .utils import are_any_inputs_missing, are_outputs_up_to_date, all_outputs_exist
from .io import Localize, Delocalize, InputSpec, InputValueSpec, OutputSpec

TOO_MANY_COMMAND_LINE_ARGS_ERROR_THRESHOLD = 500
TOO_MANY_COMMAND_LINE_ARGS_WARNING_THRESHOLD = 500


class Pipeline(ABC):
    """Pipeline represents the execution pipeline. This base class contains only generalized code that is not specific
    to any particular execution backend. It has public methods for creating Steps, as well as some private methods that
    implement the general aspects of traversing the execution graph (DAG) and transferring all steps to a specific
    execution backend.
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

        self._argument_parser = config_arg_parser

        self.name = name
        self._config_arg_parser = config_arg_parser
        self._default_output_dir = None
        self._all_steps = []
        self._unique_pipeline_instance_id = str(random.randint(10**10, 10**11))

        self._config_arg_parser_groups = {}
        config_arg_parser.add_argument("-v", "--verbose", action='count', default=0, help="Print more info")
        config_arg_parser.add_argument("-c", "--config-file", help="YAML config file path", is_config_file_arg=True)
        pipeline_group = self.get_config_arg_parser_group("pipeline")
        pipeline_group.add_argument("--dry-run", action="store_true", help="Don't run commands, just print them.")
        pipeline_group.add_argument("-f", "--force", action="store_true", help="Force execution of all steps.")
        pipeline_group.add_argument(
            "--check-file-last-modified-times",
            action="store_true",
            help="When deciding whether a Step can be skipped, instead of only checking whether all output files "
                 "already exist, also check input and output file last-modified times to make sure that all output "
                 "files are newer than all input files.")
        pipeline_group.add_argument(
            "--skip-steps-with-missing-inputs",
            action="store_true",
            help="When a Step is ready to run but has missing input file(s), the default behavior is to print an error "
                 "and exit. This arg instead causes the Step to be skipped with a warning.")

        pipeline_group.add_argument("--export-pipeline-graph", action="store_true",
            help="Export an SVG image with the pipeline flow diagram")

        notifications_group = self.get_config_arg_parser_group("notifications")
        notifications_group.add_argument("--slack-when-done", action="store_true", help="Post to Slack when execution completes")
        notifications_group.add_argument("--slack-token", env_var="SLACK_TOKEN", help="Slack token to use for notifications")
        notifications_group.add_argument("--slack-channel", env_var="SLACK_CHANNEL", help="Slack channel to use for notifications")

        gcloud_group = self.get_config_arg_parser_group("google cloud")
        gcloud_group.add_argument(
            "--gcloud-project",
            env_var="GCLOUD_PROJECT",
            help="The Google Cloud project to use for accessing requester-pays buckets, etc."
        )
        gcloud_group.add_argument(
            "--gcloud-credentials-path",
            help="Google bucket path of gcloud credentials to use in step.switch_gcloud_auth_to_user_account(..)."
                 "See the docs of that method for details.",
        )
        gcloud_group.add_argument(
            "--gcloud-user-account",
            help="Google user account to use in step.switch_gcloud_auth_to_user_account(..). See the docs of that "
                 "method for details.",
        )
        gcloud_group.add_argument(
            "--acceptable-storage-regions",
            nargs="*",
            default=("US", "US-CENTRAL1"),
            help="If specified, the pipeline will confirm that input buckets are in one of these regions "
                 "to avoid egress charges",
        )

        # validate the command-line args defined so far
        args = self.parse_known_args()
        if args.slack_when_done and (not args.slack_token or not args.slack_channel):
            config_arg_parser.error(
                "Both --slack-token and --slack-channel must be specified when --slack-when-done is used")

    def set_name(self, name):
        """Update the pipeline name"""

        self.name = name

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

    def get_config_arg_parser_group(self, group_name):
        if group_name not in self._config_arg_parser_groups:
            self._config_arg_parser_groups[group_name] = self.get_config_arg_parser().add_argument_group(group_name)
        return self._config_arg_parser_groups[group_name]

    def parse_args(self):
        """Parse command line args.

        Return:
            argparse args object.
        """
        return self._config_arg_parser.parse_args()

    def parse_known_args(self):
        """Parse command line args defined up to this point. This method can be called more than once.

        Return:
            argparse args object.
        """
        global TOO_MANY_COMMAND_LINE_ARGS_WARNING_THRESHOLD

        current_num_args = len(self._config_arg_parser._actions)
        shared_message_text = (
            "To avoid this, you can use the my_pipeline.new_step(arg_suffix=..) argument to specify a single common "
            "command-line arg suffix for Steps that run in parallel (eg. arg_suffix='step2'), while still specifying "
            "a unique Step name (such as a sample id) for each individual parallel Step.")
        if current_num_args > TOO_MANY_COMMAND_LINE_ARGS_ERROR_THRESHOLD:
            raise ValueError(
                f"The pipeline now has more than {TOO_MANY_COMMAND_LINE_ARGS_ERROR_THRESHOLD} command-line args. "
                f"{shared_message_text}")
        elif current_num_args > TOO_MANY_COMMAND_LINE_ARGS_WARNING_THRESHOLD:
            print(f"WARNING: The pipeline now has more than {TOO_MANY_COMMAND_LINE_ARGS_WARNING_THRESHOLD} "
                  f"command-line args. {shared_message_text}")
            TOO_MANY_COMMAND_LINE_ARGS_WARNING_THRESHOLD = 10**10

        # speed up this method by caching the results of parse_known_args(..) until the number of args changes
        if not hasattr(self, "_num_args") or self._num_args != current_num_args:
            args, _ = self._config_arg_parser.parse_known_args(ignore_help_args=True)
            self._cached_args = args
            self._num_args = current_num_args

        return self._cached_args

    @abstractmethod
    def new_step(self, name, step_number=None):
        """Creates a new pipeline Step. Subclasses must implement this method.

        Args:
            name (str): A short name for the step.
            step_number (int): Optional step number.
        """

    def gcloud_project(self, gcloud_project):
        print(f"WARNING: gcloud_project ignored by {type(self).__name__}")

    def cancel_after_n_failures(self, cancel_after_n_failures):
        print(f"WARNING: cancel_after_n_failures ignored by {type(self).__name__}")

    def default_image(self, default_image):
        print(f"WARNING: default_image ignored by {type(self).__name__}")

    def default_python_image(self, default_python_image):
        print(f"WARNING: default_image ignored by {type(self).__name__}")

    def default_memory(self, default_memory):
        print(f"WARNING: default_memory ignored by {type(self).__name__}")

    def default_cpu(self, default_cpu):
        print(f"WARNING: default_cpu ignored by {type(self).__name__}")

    def default_storage(self, default_storage):
        print(f"WARNING: default_storage ignored by {type(self).__name__}")

    def default_timeout(self, default_timeout):
        print(f"WARNING: default_timeout ignored by {type(self).__name__}")

    def default_output_dir(self, default_output_dir):
        """Set the default output_dir for pipeline Steps.

        Args:
            default_output_dir (str): Output directory
        """
        self._default_output_dir = default_output_dir
        return self

    @abstractmethod
    def run(self):
        """Submits a pipeline to an execution engine such as Hail Batch. Subclasses must implement this method.
        They should use this method to perform initialization of the specific execution backend and then call
        self._transfer_all_steps(..).
        """
        # run self.parse_args(..) instead of self.parse_known_args() for the 1st time to confirm that all required
        # command-line args were provided
        self._argument_parser.parse_args()

        args = self.parse_args()
        if args.export_pipeline_graph:
            if self.name is None:
                output_svg_path = "pipeline_diagram.svg"
            else:
                output_filename_prefix = re.sub("[:, ]", "_", self.name)
                output_svg_path = f"{output_filename_prefix}.pipeline_diagram.svg"
            self.export_pipeline_graph(output_svg_path=output_svg_path)
            print(f"Generated {output_svg_path}. Exiting..")
            sys.exit(0)

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

    def _check_step_graph_for_cycles(self, start_with_step=None):
        """TODO test implementation"""

        if start_with_step is None:
            for step in self._all_steps:
                # initialize variables to keep track of DAG traversal
                step.dag_traversal_all_descendents_already_visited = False
                step.dag_traversal_step_visit_count = 0

            steps = [s for s in self._all_steps if not s.has_upstream_steps()]
        else:
            steps = [start_with_step]

        for step in steps:
            step.dag_traversal_step_visit_count += 1
            if not step.dag_traversal_all_descendents_already_visited and step.dag_traversal_step_visit_count > 1:
                raise ValueError(f"Cycle detected. {next_step} was already visited")

            # push child steps
            for next_step in step._downstream_steps:
                if not next_step.dag_traversal_all_descendents_already_visited:
                    self._check_step_graph_for_cycles(start_with_step=next_step)

            step.dag_traversal_all_descendents_already_visited = True

    def _transfer_all_steps(self):
        """This method performs the core task of executing a pipeline. It traverses the execution graph (DAG) of
        user-defined Steps and decides which steps can be skipped, and which should be executed (ie. transferred to
        the execution backend).

        To decide whether a Step needs to run, this method takes into account any --skip-* command-line args,
        --force-* command-line args, whether the Step's output files already exist and are newer than all input files,
        and whether all upstream steps are also being skipped (if not, the Step will need to run).

        For Steps that need to run, this method calls step._transfer_step() to perform any backend-specific actions
        necessary to actually run the Step.

        Return:
            int: number of transferred steps
        """

        args = self.parse_args()

        self._check_step_graph_for_cycles()

        step_counters = collections.defaultdict(int)  # count steps seen (by name)
        step_run_counters = collections.defaultdict(int) # count steps run (by name)
        current_steps = [s for s in self._all_steps if not s.has_upstream_steps()]
        num_steps_transferred = 0

        # set up 'visited' boolean to track whether a Step has already been visited by the DAG traversal
        for step in self._all_steps:
            step.visited = False

        # begin traversal of DAG
        while current_steps:
            for i, step in enumerate(current_steps):
                step.visited = True
                try:
                    decided_this_step_needs_to_run = False
                    if step._cancel_this_step:
                        continue

                    elif not step._commands:
                        print(f"WARNING: No commands specified for step [{step}]. Skipping...")
                        continue

                    step_counters[step.name] += 1

                    skip_requested = any(
                        getattr(args, skip_arg_name.replace("-", "_")) for skip_arg_name in step._skip_this_step_arg_names
                    )
                    skip_requested |= any(
                        (getattr(args, run_n_arg_name.replace("-", "_")) or 10**9) < step_run_counters[step.name]
                        for run_n_arg_name in step._run_n_arg_names
                    )
                    skip_requested |= any(
                        (getattr(args, run_offset_arg_name.replace("-", "_")) or 0) > step_counters[step.name]
                        for run_offset_arg_name in step._run_offset_arg_names
                    )

                    if skip_requested:
                        print(f"Skipping {step} as requested")
                    else:
                        is_being_forced = args.force or any(
                            getattr(args, force_arg_name.replace("-", "_")) for force_arg_name in step._force_this_step_arg_names
                        )
                        if is_being_forced:
                            decided_this_step_needs_to_run = True

                        if not decided_this_step_needs_to_run:
                            all_upstream_steps_skipped = all(s._is_being_skipped for s in step._upstream_steps)
                            if not all_upstream_steps_skipped:
                                if args.verbose:
                                    print(f"Running {step} because upstream step is going to run.")
                                decided_this_step_needs_to_run = True
                            elif args.skip_steps_with_missing_inputs and are_any_inputs_missing(step, verbose=args.verbose):
                                # only do this check if upstream steps are being skipped. Otherwise, input files may not exist yet.
                                continue  # skip this step

                        if not decided_this_step_needs_to_run:
                            if not args.check_file_last_modified_times:
                                if len(step._output_specs) == 0:
                                    if args.verbose:
                                        print(f"Running {step}. No outputs specified.")
                                    decided_this_step_needs_to_run = True
                                elif not all_outputs_exist(
                                        step,
                                        only_check_the_cache=step._all_outputs_precached,
                                        verbose=args.verbose):
                                    if args.verbose:
                                        print(f"Running {step} because some output(s) don't exist yet.")
                                    decided_this_step_needs_to_run = True
                            else:
                                if not are_outputs_up_to_date(
                                        step,
                                        only_check_the_cache=step._all_inputs_precached and step._all_outputs_precached,
                                        verbose=args.verbose):
                                    if args.verbose:
                                        print(f"Running {step} because some output(s) don't exist yet or are not up-to-date.")

                                    decided_this_step_needs_to_run = True

                    if not decided_this_step_needs_to_run and not skip_requested:
                        print(f"Skipping {step}. The {len(step._output_specs)} output" +
                              ("s already exist" if len(step._output_specs) > 1 else " already exists") +
                              ("." if args.check_file_last_modified_times else
                               " and are up-to-date." if len(step._output_specs) > 1 else " and is up-to-date"))
                        if args.verbose > 0:
                            print(f"Outputs:")
                            for o in step._output_specs:
                                print(f"       {o}")

                finally:
                    if decided_this_step_needs_to_run:
                        print(("%-120s" % f"==> Running {step}") + (
                            f"[#{i+1}]" if len(current_steps) > 1 else ""))
                        step._is_being_skipped = False
                        try:
                            step._transfer_step()
                            step_run_counters[step.name] += 1
                            num_steps_transferred += 1
                        except Exception as e:
                            print(f"ERROR: while transferring step {step}: {e}. Skipping..")
                            step._is_being_skipped = True
                    else:
                        step._is_being_skipped = True

            # next, process all steps that depend on the previously-completed steps
            next_steps = []
            for step in current_steps:
                for downstream_step in step._downstream_steps:
                    if downstream_step in next_steps:
                        # when multiple current steps share the same downstream step, avoid adding it multiple times
                        continue

                    if any(not s.visited for s in downstream_step._upstream_steps):
                        # if any of the steps this downstream step depends on haven't been processed yet, wait for that
                        continue
                    next_steps.append(downstream_step)

            current_steps = next_steps

        # clear all steps that have been transferred
        self._all_steps = []

        return num_steps_transferred

    def _generate_post_to_slack_command(self, message, channel=None, slack_token=None):
        """Generates the command which posts to Slack

        Args:
            message (str): The message to post.
            channel (str): The Slack channel to post to.
            slack_token (str): Slack auth token.

        Return:
            str: command that posts the given message to Slack
        """

        args = self.parse_known_args()
        slack_token = slack_token or args.slack_token
        if not slack_token:
            raise ValueError("slack token not provided")
        channel = channel or args.slack_channel
        if not channel:
            raise ValueError("slack channel not specified")

        return f"""python3 <<EOF
from slacker import Slacker
slack = Slacker("{slack_token}")
response = slack.chat.post_message("{channel}", "{message}", as_user=False, icon_emoji=":bell:", username="step-pipeline-bot")
print(response.raw)
EOF"""

    def precache_file_paths(self, glob_path):
        """This method is an alias for the check_input_glob(..) method"""

        return self.check_input_glob(glob_path)

    def check_input_glob(self, glob_path):
        """This method is useful for checking the existence of multiple input files and caching the results.
        Input file(s) to this Step using glob syntax (ie. using wildcards as in `gs://bucket/**/sample*.cram`)

        Args:
            path (str): local file path or gs:// Google Storage path. The path can contain wildcards (*).

        Return:
            list: List of metadata dicts like::

            [
                {
                    'path': 'gs://bucket/dir/file.bam.bai',
                    'size_bytes': 2784,
                    'modification_time': 'Wed May 20 12:52:01 EDT 2020',
                },
            ]

        """
        try:
            return _file_stat__cached(glob_path)
        except FileNotFoundError as e:
            return []

    def export_pipeline_graph(self, output_svg_path=None):
        """Renders the pipeline execution graph diagram based on the Steps defined so far.

        Args:
            output_svg_path (str): Path where to write the SVG image with the execution graph diagram. If not specified,
                it will be based on the pipeline name.
        """
        if not output_svg_path:
            output_svg_path = re.sub("[ :]", "_", self.name) + ".pipeline_diagram.svg"

        try:
            import pygraphviz as pgv
        except ImportError as e:
            print("Error: pygraphviz is required for this feature. Please install it first.")
            return

        G = pgv.AGraph(strict=False, directed=True)
        G.node_attr["shape"] = "none"
        G.graph_attr["rankdir"] = "TB"

        # start with steps that have no upstream steps
        current_steps = [s for s in self._all_steps if not s.has_upstream_steps()]
        while current_steps:
            previously_seen_step_names = set()
            for step in current_steps:
                if step.name in previously_seen_step_names:
                    continue
                previously_seen_step_names.add(step.name)

                step_label = step.name
                if step.step_number is not None:
                    step_label = f"#{step.step_number}: {step_label}"

                step_label = f"""
                <TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">
                <TR><TD ALIGN="LEFT"><B>{step_label}</B></TD></TR>"""

                inputs_and_outputs = (
                    [("Input", step._input_specs)] if step._input_specs else []
                ) + (
                    [("Output", step._output_specs)] if step._output_specs else []
                )
                for input_or_output, spec_list in inputs_and_outputs:
                    for i, spec in enumerate(spec_list):
                        prefix = input_or_output
                        if len(spec_list) > 1:
                            prefix += f" {i + 1}"
                        prefix += ": "
                        step_label += f"""<TR><TD ALIGN="LEFT"><FONT POINT-SIZE="11">{prefix}<B>{spec.name or spec.filename}</B></FONT></TD></TR>"""

                step_label += "</TABLE>"

                if step._input_specs or step._output_specs:
                    step_label = f"<{step_label}>"

                G.add_node(f"node_{step._step_id}", label=step_label, shape="none")

                for upstream_step in step._upstream_steps:
                    G.add_edge(f"node_{upstream_step._step_id}", f"node_{step._step_id}")

            # next, process all steps that depend on the previously-completed steps
            current_steps = [downstream_step for step in current_steps for downstream_step in step._downstream_steps]

        G.draw(output_svg_path, prog="dot")


class Step(ABC):
    """Represents a set of commands or sub-steps which together produce some output file(s), and which can be
    skipped if the output files already exist (and are newer than any input files unless a --force arg is used).
    A Step's input and output files must be stored in some persistant location, like a local disk or GCS.

    Using Hail Batch as an example, a Step typically corresponds to a single Hail Batch Job. Sometimes a Job can be
    reused to run multiple steps (for example, where step 1 creates a VCF and step 2 tabixes it).
    """
    _STEP_ID_COUNTER = 0
    _USED_FORCE_ARG_SUFFIXES = set()
    _USED_SKIP_ARG_SUFFIXES = set()
    _USED_RUN_SUBSET_ARG_SUFFIXES = set()

    def __init__(self,
                 pipeline,
                 name,
                 step_number=None,
                 arg_suffix=None,
                 output_dir=None,
                 localize_by=None,
                 delocalize_by=None,
                 add_force_command_line_args=True,
                 add_skip_command_line_args=True,
                 add_run_subset_command_line_args=True,
                 all_inputs_precached=False,
                all_outputs_precached=False,
        ):
        """Step constructor

        Args:
            pipeline (Pipeline): The Pipeline object representing the current pipeline.
            name (str): A short name for this step
            step_number (int): If specified, --skip-step{step_number} and --force-step{step_number} command-line args
                will be created.
            arg_suffix (str): If specified, --skip-{arg_suffix} and --force-{arg_suffix} command-line args will be
                created.
            output_dir (str): If specified, this will be the default output directory used by Step outputs.
            localize_by (Localize): If specified, this will be the default Localize approach used by Step inputs.
            delocalize_by (Delocalize): If specified, this will be the default Delocalize approach used by Step outputs.
            add_force_command_line_args (bool): Whether to add command line args for forcing execution of this Step.
            add_skip_command_line_args (bool): Whether to add command line args for skipping execution of this Step.
            add_run_subset_command_line_args (bool): Whether to add command line args for running a subset of
                parallel jobs from this Step (--run-n-step1, --run-offset-step1).
            all_inputs_precached (bool): If True, all inputs for this Step will be assumed to have been checked and
                pre-cached already via call(s) to pipeline.precache_file_paths(..). This allows for much faster
                processing when deciding which steps need to run and which can be skipped because their outputs
                already exist and are newer than their inputs.
            all_outputs_precached (bool): Same as the  all_inputs_precached argument, but for outputs.
        """
        self._pipeline = pipeline
        self.name = name
        self.step_number = step_number
        self.arg_suffix = arg_suffix
        self._output_dir = output_dir

        self._localize_by = localize_by
        self._delocalize_by = delocalize_by

        self._input_specs = []
        self._input_value_specs = []
        self._output_specs = []
        self._commands = []   # used for BashJobs

        #self._calls = []  # use for PythonJobs (Not yet implemented)
        #self._substeps = []  # steps that are contained within this step (Not yet implemented)

        self._upstream_steps = []  # this Step depends on these Steps
        self._downstream_steps = []  # Steps that depend on this Step

        self._cancel_this_step = False  # records whether the user changed their mind and wants to cancel this step.
        self._is_being_skipped = False  # records whether this Step is being skipped

        self._force_this_step_arg_names = []
        self._skip_this_step_arg_names = []
        self._run_n_arg_names = []
        self._run_offset_arg_names = []
        self._all_inputs_precached = all_inputs_precached
        self._all_outputs_precached = all_outputs_precached

        Step._STEP_ID_COUNTER += 1
        self._step_id = Step._STEP_ID_COUNTER  # this id is unique to each Step object

        self._unique_step_instance_id = str(random.randint(10**10, 10**11))

        # define command line args for skipping or forcing execution of this step
        command_line_arg_suffixes = set()
        def cleanup_arg_suffix(suffix):
            return suffix.replace(" ", "-").replace(":", "").replace("_", "-")

        if arg_suffix:
            command_line_arg_suffixes.add(cleanup_arg_suffix(arg_suffix))
        elif name:
            command_line_arg_suffixes.add(cleanup_arg_suffix(name))

        if step_number is not None:
            if not isinstance(step_number, (int, float)):
                raise ValueError(f"step_number must be an integer or a float rather than '{step_number}'")
            command_line_arg_suffixes.add(f"step{step_number}")

        for suffix in command_line_arg_suffixes:
            if add_force_command_line_args:
                self._force_this_step_arg_names.append(f"force_{suffix}")
                if suffix not in Step._USED_FORCE_ARG_SUFFIXES:
                    self._pipeline.get_config_arg_parser_group("pipeline").add_argument(
                        f"--force-{suffix}",
                        help=f"Force execution of '{name}'.",
                        action="store_true",
                    )
                    Step._USED_FORCE_ARG_SUFFIXES.add(suffix)

            if add_skip_command_line_args:
                self._skip_this_step_arg_names.append(f"skip_{suffix}")
                if suffix not in Step._USED_SKIP_ARG_SUFFIXES:
                    self._pipeline.get_config_arg_parser_group("pipeline").add_argument(
                        f"--skip-{suffix}",
                        help=f"Skip '{name}' even if --force is used.",
                        action="store_true",
                    )
                    Step._USED_SKIP_ARG_SUFFIXES.add(suffix)

            if add_run_subset_command_line_args:
                self._run_n_arg_names.append(f"run_n_{suffix}")
                self._run_offset_arg_names.append(f"run_offset_{suffix}")
                if suffix not in Step._USED_RUN_SUBSET_ARG_SUFFIXES:
                    self._pipeline.get_config_arg_parser_group("pipeline").add_argument(
                        f"--run-n-{suffix}",
                        help=f"Run only this many parallel jobs for '{name}' even if --force is used. This can be "
                             f"useful for test-running a pipeline.",
                        type=int,
                    )
                    self._pipeline.get_config_arg_parser_group("pipeline").add_argument(
                        f"--run-offset-{suffix}",
                        help=f"Skip the first this many parallel jobs from '{name}' even if --force is used. This can "
                             f"be useful for test-running a pipeline, especially when used with --run-n-..",
                        type=int,
                    )
                    Step._USED_RUN_SUBSET_ARG_SUFFIXES.add(suffix)

    def __eq__(self, other):
        return isinstance(other, Step) and self._step_id == other._step_id

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return self._step_id

    def name(self, name):
        """Set the short name for this Step.

        Args:
            name (str): Name
        """
        self.name = name

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

    def input_glob(self, glob_path, name=None, localize_by=None):
        """Specify input file(s) to this Step using glob syntax (ie. using wildcards as in `gs://bucket/**/sample*.cram`)

        Args:
            glob_path (str): The path of the input file(s) or directory to localize, optionally including wildcards.
            name (str): Optional name for this input.
            localize_by (Localize): How this path should be localized.

        Return:
            InputSpec: An object that describes the specified input file or directory.
        """
        return self.input(glob_path, name=name, localize_by=localize_by)

    def input_value(self, value=None, name=None, input_type=None):
        """Specify a Step input that is something other than a file path.

        Args:
            value: The input's value.
            name (str): Optional name for this input.
            input_type (InputType): The value's type.

        Return:
            InputValueSpec: An object that contains the input value, name, and type.
        """
        input_value_spec = InputValueSpec(
            value=value,
            name=name,
            input_type=input_type,
        )

        self._input_value_specs.append(input_value_spec)

        return input_value_spec

    def input(self, source_path=None, name=None, localize_by=None):
        """Specifies an input file or directory.

        Args:
            source_path (str): Path of input file or directory to localize.
            name (str): Optional name for this input.
            localize_by (Localize): How this path should be localized.
        Return:
            InputSpec: An object that describes the specified input file or directory.
        """
        localize_by = localize_by or self._localize_by

        # validate inputs
        if source_path is not None and not isinstance(source_path, str):
            raise ValueError(f"source_path '{source_path}' has type {type(source_path).__name__} instead of string")

        if not source_path.startswith("gs://") and localize_by in (
                Localize.GSUTIL_COPY,
        ):
            raise ValueError(f"source_path '{source_path}' doesn't start with gs://")

        if not source_path.startswith("gs://") and not source_path.startswith("hail-az://") and localize_by in (
                Localize.HAIL_BATCH_CLOUDFUSE_VIA_TEMP_BUCKET,
        ):
            raise ValueError(f"source_path '{source_path}' doesn't start with gs:// or hail-az://")

        input_spec = InputSpec(
            source_path=source_path,
            name=name,
            localize_by=localize_by,
            localization_root_dir=self._pipeline._get_localization_root_dir(localize_by),
        )

        input_spec = self._preprocess_input_spec(input_spec)
        self._input_specs.append(input_spec)

        return input_spec

    def inputs(self, source_path, *source_paths, name=None, localize_by=None):
        """Specifies one or more input file or directory paths.

        Args:
            source_path (str): Path of input file or directory to localize.
            name (str): Optional name to apply to all these inputs.
            localize_by (Localize): How these paths should be localized.

        Return:
            list: A list of InputSpec objects that describe these input files or directories. The list will contain
                one entry for each passed-in source path.
        """
        source_paths_flat_list = []
        for source_path in [source_path, *source_paths]:
            if isinstance(source_path, str):
                source_paths_flat_list.append(source_path)
            else:
                source_paths_flat_list += list(source_path)

        input_specs = []
        for source_path in source_paths_flat_list:
            input_spec = self.input(source_path, name=name, localize_by=localize_by)
            input_specs.append(input_spec)

        return input_specs

    def use_the_same_inputs_as(self, other_step, localize_by=None):
        """Copy the inputs of another step, while optionally changing the localize_by approach. This is a utility method
        to make it easier to specify inputs for a Step that is very similar to a previously-defined step.

        Args:
            other_step (Step): The Step object to copy inputs from.
            localize_by (Localize): Optionally specify how these inputs should be localized. If not specified, the value
                from other_step will be reused.

        Return:
             list: A list of new InputSpec objects that describe the inputs copied from other_step. The returned list
                will contain one entry for each input of other_step.
        """
        localize_by = localize_by or self._localize_by

        input_specs = []
        for other_step_input_spec in other_step._input_specs:
            input_spec = self.input(
                source_path=other_step_input_spec.source_path,
                name=other_step_input_spec.name,
                localize_by=localize_by or other_step_input_spec.localize_by,
            )

            input_specs.append(input_spec)

        if len(input_specs) == 1:
            return input_specs[0]
        else:
            return input_specs

    def use_previous_step_outputs_as_inputs(self, previous_step, localize_by=None):
        """Define Step inputs to be the output paths of an upstream Step and explicitly mark this Step as downstream of
        previous_step by calling self.depends_on(previous_step).

        Args:
            previous_step (Step): A Step that's upstream of this Step in the pipeline.
            localize_by (Localize): Specify how these inputs should be localized. If not specified, the default
                localize_by value for the pipeline will be used.
        Return:
             list: A list of new InputSpec objects that describe the inputs defined based on the outputs of
             previous_step. The returned list will contain one entry for each output of previous_step.
        """
        self.depends_on(previous_step)

        localize_by = localize_by or self._localize_by

        input_specs = []
        for output_spec in previous_step._output_specs:
            input_spec = self.input(
                source_path=output_spec.output_path,
                name=output_spec.name,
                localize_by=localize_by,
            )

            input_specs.append(input_spec)

        if len(input_specs) == 1:
            return input_specs[0]
        else:
            return input_specs

    def output_dir(self, path):
        """If an output path is specified as a relative path, it will be relative to this dir.

        Args:
            path (str): Directory path.
        """
        self._output_dir = path

    def output(self, local_path, output_path=None, output_dir=None, name=None, delocalize_by=None, optional=False):
        """Specify a Step output file or directory.

        Args:
            local_path (str): The file or directory path within the execution container's file system.
            output_path (str): Optional destination path to which the local_path should be delocalized.
            output_dir (str): Optional destination directory to which the local_path should be delocalized.
                It is expected that either output_path will be specified, or an output_dir value will be provided as an
                argument to this method or previously (such as by calling the step.output_dir(..) setter method).
                If both output_path and output_dir are specified and output_path is a relative path, it is interpreted
                as being relative to output_dir.
            name (str): Optional name for this output.
            delocalize_by (Delocalize): How this path should be delocalized.
            optional (bool): If True, this output is considered optional and, although it will be delocalized, steps
                that didn't produce this output will still be skipped even if this output is missing. This is useful
                for modifying existing pipelines to output additional files (eg. log files) without this triggering a
                rerun of previously steps that completed previously without generating these files.

        Returns:
            OutputSpec: An object describing this output.
        """

        delocalize_by = delocalize_by or self._delocalize_by
        if delocalize_by is None:
            raise ValueError("delocalize_by not specified")

        output_spec = OutputSpec(
            local_path=local_path,
            output_dir=output_dir or self._output_dir,
            output_path=output_path,
            name=name,
            delocalize_by=delocalize_by,
            optional=optional,
        )

        self._preprocess_output_spec(output_spec)

        self._output_specs.append(output_spec)

        return output_spec

    def outputs(self, local_path, *local_paths, output_dir=None, name=None, delocalize_by=None):
        """Define one or more outputs.

        Args:
            local_path (str): The file or directory path within the execution container's file system.
            output_dir (str): Optional destination directory to which the given local_path(s) should be delocalized.
            name (str): Optional name for the output(s).
            delocalize_by (Delocalize): How the path(s) should be delocalized.

        Returns:
            list: A list of OutputSpec objects that describe these outputs. The list will contain one entry for each passed-in path.
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
        if isinstance(upstream_step, Step):
            if upstream_step not in self._upstream_steps:
                self._upstream_steps.append(upstream_step)
            if self not in upstream_step._downstream_steps:
                upstream_step._downstream_steps.append(self)

        elif isinstance(upstream_step, list):
            for _upstream_step in upstream_step:
                if _upstream_step not in self._upstream_steps:
                    self._upstream_steps.append(_upstream_step)
                if self not in _upstream_step._downstream_steps:
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
        if self.step_number is not None and self.name  is not None:
            s += ": "
        if self.name is not None:
            s += self.name

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
        if not hasattr(self, "_already_installed_slacker"):
            self.command("python3 -m pip install slacker")
            self._already_installed_slacker = True

        self.command(self._pipeline._generate_post_to_slack_command(message, channel=channel, slack_token=slack_token))

    def gcloud_auth_activate_service_account(self):
        """Utility method to active gcloud auth using the Hail Batch-provided service account"""

        if hasattr(self, "_switched_gcloud_auth_to_service_account"):
            return
        self.command(f"gcloud auth activate-service-account --key-file /gsa-key/key.json")
        self._switched_gcloud_auth_to_service_account = True

    def switch_gcloud_auth_to_user_account(self, gcloud_credentials_path=None, gcloud_user_account=None,
                                           gcloud_project=None, debug=False):
        """This method adds commands to this Step to switch gcloud auth from the Batch-provided service
        account to the user's personal account.

        This is useful if subsequent commands need to access google buckets that to which the user's personal account
        has access but to which the Batch service account cannot be granted access for whatever reason.

        For this to work, you must first::

        1) create a google bucket that only you have access to - for example: gs://weisburd-gcloud-secrets/
        2) on your local machine, make sure you're logged in to gcloud by running::

           gcloud auth login

        3) copy your local ~/.config directory (which caches your gcloud auth credentials) to the secrets bucket from step 1::

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

        if hasattr(self, "_switched_gcloud_auth_to_user_account"):
            return

        args = self._pipeline.parse_known_args()
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
        
        self.gcloud_auth_activate_service_account()
        self.command(f"gsutil -m cp -r {os.path.join(gcloud_credentials_path, '.config')} /tmp/")
        self.command(f"rm -rf ~/.config")
        self.command(f"mv /tmp/.config ~/")
        self.command(f"gcloud config set account {gcloud_user_account}")
        if gcloud_project:
            self.command(f"gcloud config set project {gcloud_project}")
        self.command(f"export GOOGLE_APPLICATION_CREDENTIALS=$(find ~/.config/ -name 'adc.json')")

        if debug:
            self.command(f"gcloud auth list")  # print auth list again to check if 'gcloud config set account' succeeded

        self._switched_gcloud_auth_to_user_account = True

    @abstractmethod
    def _get_supported_localize_by_choices(self):
        """Returns set of Localize options supported by this pipeline"""
        return set()

    @abstractmethod
    def _get_supported_delocalize_by_choices(self):
        """Returns set of Delocalize options supported by this pipeline"""
        return set()

    @abstractmethod
    def _preprocess_input_spec(self, input_spec):
        """This method is called by step.input(..) immediately when the input is first specified, regardless of whether
        the Step runs or not. It should perform simple checks of the input_spec that are fast and don't require a
        network connection, but that catch simple errors such as incorrect source path syntax.
        Step subclasses must implement this method.

        Args:
            input_spec (InputSpec): The input to preprocess.

        Return:
            input_spec (InputSpec): A potentially-updated input_spec.
        """
        if input_spec.localize_by not in self._get_supported_localize_by_choices():
            raise ValueError(f"Unexpected input_spec.localize_by value: {input_spec.localize_by}")

        return input_spec

    @abstractmethod
    def _transfer_input_spec(self, input_spec):
        """When a Step isn't skipped and is being transferred to the execution backend, this method is called for
        each input to the Step. It should localize the input into the Step's execution container using the approach
        requested by the user via the localize_by parameter.

        Args:
            input_spec (InputSpec): The input to localize.
        """
        if input_spec.localize_by not in self._get_supported_localize_by_choices():
            raise ValueError(f"Unexpected input_spec.localize_by value: {input_spec.localize_by}")

    @abstractmethod
    def _preprocess_output_spec(self, output_spec):
        """This method is called by step.output(..) immediately when the output is first specified, regardless of
        whether the Step runs or not. It should perform simple checks of the output_spec that are fast and don't
        require a network connection, but that catch simple errors such as incorrect output path syntax.
        Step subclasses must implement this method.

        Args:
            output_spec (OutputSpec): The output to preprocess.
        """
        if output_spec.delocalize_by not in self._get_supported_delocalize_by_choices():
            raise ValueError(f"Unexpected output_spec.delocalize_by value: {output_spec.delocalize_by}")

    @abstractmethod
    def _transfer_output_spec(self, output_spec):
        """When a Step isn't skipped and is being transferred to the execution backend, this method will be called for
        each output of the Step. It should delocalize the output from the Step's execution container to the requested
        destination path using the approach requested by the user via the delocalize_by parameter.

        Args:
            output_spec (OutputSpec): The output to delocalize.
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

    def cancel(self):
        """Signals that this Step shouldn't be run after all. Sometimes it is convenient to make this decision after a
        Step has already been created, but before the pipeline is executed.
        """
        self._cancel_this_step = True
        self._commands = []


    def skip(self):
        """Alias for self.cancel()"""
        self.cancel()

