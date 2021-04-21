"""
This batch pipeline wrapper implements a pipeline "Step" abstraction which makes it easier to write pipelines
and reduce repetitive code.

A single Step contains a set of commands or Batch Jobs which together produce some output file(s), and which can be
skipped if the output files already exist (and are newer than any input files unless a --force arg is used).

By default, a Step corresponds to a single Batch Job, but in some cases
one Batch Job may be reused for multiple steps (for example, step 1 creates a VCF and step 2 tabixes it), or
alternatively, a single Step may contain multiple Batch Jobs or even a large subgraph of the overall DAG.

Besides doing these checks on inputs/outputs, a Step will automatically define argparse args for skipping or forcing
the step's execution, localize/delocalize the input/output files, and possibly collect timing and profiling info
on memory & disk use (such as starting a background command to record free memory and disk use every 10 seconds,
localizing/delocalizing a .tsv file that accumulates memory and other stats for all steps)

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
"""


import argparse
from abc import ABC, abstractmethod

import configargparse
import contextlib
import hailtop.batch as hb

from hailtop.batch.job import Job



class _PipelineRunner(ABC):

    def __init__(self, pipeline):
        self.pipeline = pipeline

    def are_outputs_up_to_date(self, step):
        """Returns True if all outputs already exist and are newer than all inputs"""


    @abstractmethod
    def _transfer_step(self, step):
        """Handles all interactions with the execution system. This method is called if the step does need to run.
        """

    def _transfer_pipeline(self, args, pipeline):

        steps_to_run_next = [s for s in pipeline._all_steps if not s.depends_on_other_steps()]
        while steps_to_run_next:
            for step in steps_to_run_next:
                step.needs_to_run = args.force or any(s.needs_to_run for s in step.depends_on_these_steps) or not self.are_outputs_up_to_date(step)
                # TODO check if step needs to run
                # TODO add commands for localizing, delocalizing files
                # TODO merge step that reuses jobs
                if step.needs_to_run:
                    self._transfer_step(step)
            steps_to_run_next = [s for step in steps_to_run_next for s in step.steps_that_depend_on_this_step]
        for s in steps_to_run_next:
            self._transfer_step(s)

    @abstractmethod
    def run(self):
        """Submits the pipeline to an execution environment such as Batch, Terra, SGE, etc."""


class _BatchPipelineRunner(_PipelineRunner):

    def __init__(self, pipeline):
        super.__init__(pipeline)

    def _transfer_step(self, step):
        """Handles all interactions with the execution system. This method is called if the step does need to run.
        """
        if step.reuse_job_from_step and step.reuse_job_from_step.job:
            step.job = step.reuse_job_from_step.job
        else:
            step.job = hb.Job()

        for upstream_step in step.depends_on_these_steps:
            if upstream_step.job:
                step.job.depends_on(upstream_step.job)

        for command in step.commands:
            step.job.command(command)


    def run(self, pipeline):

        # pass pipeline to the Batch service
        args = pipeline.get_argument_parser().parse_args()
        try:
            if args.local:
                backend = hb.LocalBackend() if args.raw else hb.LocalBackend(gsa_key_file=args.gsa_key_file)
            else:
                backend = hb.ServiceBackend(billing_project=args.batch_billing_project, bucket=args.batch_temp_bucket)

            batch = hb.Batch(backend=backend, name=name)
            batch.batch_utils_temp_bucket = args.batch_temp_bucket
            self._transfer_pipeline(args, pipeline, batch)
            batch.run(dry_run=args.dry_run, verbose=args.verbose)
        finally:
            if isinstance(backend, hb.ServiceBackend):
                backend.close()

class _Step:
    """Represents a set of commands or Batch Jobs which together produce some output file(s), and which can be
    skipped if the output files already exist (and are newer than any input files unless a --force arg is used).
    A Step's input and output files must exist in some persistent storage - like Google Cloud Storage.

    By default, a Step corresponds to a single Batch Job, but in some cases
    one Batch Job may be reused for multiple steps (for example, step 1 creates a VCF and step 2 tabixes it).
    Also, Steps can be nested so that a single Step contains multiple Steps or even a large subgraph of the overall DAG.

    Besides doing these checks on inputs/outputs, a Step will automatically define argparse args for skipping or forcing
    the step's execution, localize/delocalize the input/output files, and possibly collect timing and profiling info
    on memory & disk use (such as starting a background command to record free memory and disk use every 10 seconds,
    localizing/delocalizing a .tsv file that accumulates memory and other stats for all steps)
    """

    def __init__(self, name, pipeline):
        self.pipeline = pipeline
        self.pipeline._register_step(self)

        self.inputs = []
        self.outputs = []
        self.commands = []

        self.depends_on_these_steps = []
        self.steps_that_depend_on_this_step = []

        # A job represents a set of commands executed sequentially in the same execution environment (same memory and disk)
        self.reuse_job_from_step = None

        self.name = name

        # used when submitting graph to execution graph
        self._can_be_skipped = False
        self._job = None
        self._already_processed = False

    def next_step(self, name, reuse_job=False, cpu=None, memory=None, disk=None):
        """Creates a new dependent step"""
        s = _Step(name)
        s.depends_on(self, reuse_job=reuse_job)
        return s

    def command(self, command):
        """Adds a command"""
        self.commmands.append(command)

    def input_glob(self, glob, name=None, use_gcf_fuse=False):
        pass

    def input(self, path, name=None, use_gcf_fuse=False):
        pass

    def localize(self, path, name=None, use_gcf_fuse=False):
        """
        """
    def inputs_from(self, previous_step):
        pass

    def output(self, path):
        pass

    def depends_on(self, step, reuse_job=False):
        if isinstance(step, _Step):
            self.depends_on_these_steps.append(step)
            step.steps_that_depend_on_this_step.append(self)

            self.reuse_job_from_step=step

        elif isinstance(step, list):
            if reuse_job:
                raise ValueError("step cannot be a list when reuse_job=True")

            self.depends_on_these_steps.extend(step)
            for upstream_step in step:
                upstream_step.steps_that_depend_on_this_step.append(self)

    def depends_on_other_steps(self):
        return len(self.depends_on_these_steps) > 0


# defines a group of steps which together take some input(s) and produce some output(s). They can all be skipped as a group, and intermediate output files can be cleaned up after the group completes successfully. 
class _StepGroup:
    pass


class _Pipeline:
    def __init__(self, argument_parser: argparse.ArgumentParser = None):
        if argument_parser is None:
            argument_parser = configargparse.ArgumentParser(
                formatter_class=configargparse.ArgumentDefaultsRawHelpFormatter,
                default_config_files=["~/.batch_wrapper"])
        self._set_argument_parser(argument_parser)

        self._all_steps = []

    def _set_argument_parser(self, parser: argparse.ArgumentParser):
        parser.add_argument("--batch-billing-project", help="This billing project will be charged when running jobs on "
            "the Batch cluster. To set up a billing project, email the hail team.")
        parser.add_argument("--batch-temp-bucket", help="A bucket where Batch can store temp files. The Batch service "
            "account must have Admin access to this bucket. To get the name of your Batch service account, "
            "go to https://auth.hail.is/user. Then, to grant Admin permissions, run "
            "gsutil iam ch serviceAccount:[SERVICE_ACCOUNT_NAME]:objectAdmin gs://[BUCKET_NAME]")
        parser.add_argument("--dry-run", action="store_true", help="Don't run commands, just print them.")
        parser.add_argument("-v", "--verbose", action="store_true", help="Verbose log output.")

        local_or_cluster_grp = parser.add_mutually_exclusive_group(required=True)
        local_or_cluster_grp.add_argument("--local", action="store_true", help="run locally")
        local_or_cluster_grp.add_argument("--cluster", action="store_true", help="submit to cluster")

        self.parser = parser


    def get_argument_parser(self) -> argparse.ArgumentParser:
        return self.parser


    def new_step(self, name, cpu=None, memory=None, disk=None):
        """Creates a new Step.

        This is the only way to create Steps that don't depend on other steps.
        """
        step = _Step(name)
        return step


    def _register_step(self, step):
        self._all_steps.append(step)



@contextlib.contextmanager
def batch_pipeline(name=None, desc=None):
    """Wrapper for creating, running, and then closing a Batch run.

    :param name: (optional) pipeline name that will appear
    :param batch_name: (optional) batch label which will show up in the Batch web UI

    Usage:
        with run_batch(args) as batch:
            ... batch job definitions ...
    """
    bp = _Pipeline()

    yield bp

    bp.execute()


"""
def init_arg_parser(
        default_billing_project="tgg-rare-disease",
        default_temp_bucket="macarthurlab-cromwell",
        default_cpu=1,
        default_memory=3.75,
        parser=configargparse.ArgumentParser(formatter_class=configargparse.ArgumentDefaultsRawHelpFormatter),
        gsa_key_file=None,
):
    "Initializes and returns an argparse instance with common pipeline args pre-defined."

    local_or_cluster_grp = parser.add_mutually_exclusive_group(required=True)
    local_or_cluster_grp.add_argument("--local", action="store_true", help="Batch: run locally")
    local_or_cluster_grp.add_argument("--cluster", action="store_true", help="Batch: submit to cluster")
    parser.add_argument("-r", "--raw", action="store_true", help="Batch: run directly on the machine, without using a docker image")

    parser.add_argument("--gsa-key-file", default=gsa_key_file, help="Batch: path of gcloud service account .json "
            "key file. If provided, Batch will mount this file into the docker image so gcloud commands can run as this service account.")
    parser.add_argument("--batch-billing-project", default=default_billing_project, help="Batch: this billing project will be "
                                                                                         "charged when running jobs on the Batch cluster. To set up a billing project name, contact the hail team.")
    parser.add_argument("--batch-temp-bucket", default=default_temp_bucket, help="Batch: bucket where it stores temp "
                                                                                 "files. The batch service-account must have Admin permissions for this bucket. These can be added by running "
                                                                                 "gsutil iam ch serviceAccount:[SERVICE_ACCOUNT_NAME]:objectAdmin gs://[BUCKET_NAME]")
    parser.add_argument("-t", "--cpu", type=float, default=default_cpu, choices=[0.25, 0.5, 1, 2, 4, 8, 16], help="Batch: number of CPUs (eg. 0.5)")
    parser.add_argument("-m", "--memory", type=float, default=default_memory, help="Batch: memory in gigabytes (eg. 3.75)")
    parser.add_argument("-f", "--force", action="store_true", help="Recompute and overwrite cached or previously computed data")
    parser.add_argument("--start-with", type=int, help="Start from this step in the pipeline")
    parser.add_argument("--dry-run", action="store_true", help="Don't run commands, just print them.")
    parser.add_argument("--verbose", action="store_true", help="Verbose log output.")
    return parser
"""
