"""This module contains Cromwell/Terra-specific extensions of the Pipeline and Step classes"""
import re
from .constants import Backend
from .pipeline import Pipeline, Step, Localize, Delocalize


def _remove_special_chars(name):
    return re.sub("[\W]", name, " ").replace(".", " ").replace("-", " ").replace(":", " ").replace("_", " ")


def _to_pascal_case(s):
    if s is None:
        raise ValueError("_to_pascal_case input is None")
    return _remove_special_chars(s).title().replace(" ", "")


def _to_camel_case(s):
    if s is None:
        raise ValueError("_to_camel_case input is None")
    s = _to_pascal_case(s)
    return s[0].lower() + s[1:]


class WdlPipeline(Pipeline):
    """This class extends the Pipeline class to add support for generating a WDL and will later add support for
    running it using Cromwell or Terra.
    """

    def __init__(self, name=None, config_arg_parser=None, backend=Backend.TERRA):
        """WdlPipeline constructor

        Args:
            name (str): Pipeline name
            config_arg_parser (configargparse): The configargparse.ArgumentParser object to use for defining
                command-line args
            backend (Backend): Either Backend.TERRA or Backend.CROMWELL.
        """
        super().__init__(name=name, config_arg_parser=config_arg_parser)

        self._backend = backend

        wdl_args = config_arg_parser.add_argument_group("wdl")
        default_output_path = f"{_to_pascal_case(name)}.wdl" if name else "pipeline.wdl"
        wdl_args.add_argument("--wdl-output-path", help="Output path of .wdl file", default=default_output_path)

    @property
    def backend(self):
        """Returns either Backend.CROMWELL or Backend.TERRA"""
        return self._backend

    def new_step(
        self,
        name=None,
        step_number=None,
        depends_on=None,
        image=None,
        cpu=None,
        memory=None,
        storage=None,
        localize_by=Localize.COPY,
        delocalize_by=Delocalize.COPY,
        **kwargs,
    ):
        """Creates a new pipeline Step.

        Args:
            name (str): A short name for this Step.
            step_number (int): Optional Step number which serves as another alias for this step in addition to name.
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
                bytes.
            localize_by (Localize): If specified, this will be the default Localize approach used by Step inputs.
            delocalize_by (Delocalize): If specified, this will be the default Delocalize approach used by Step outputs.
            **kwargs: other keyword args can be provided, but are ignored.

        Return:
            WdlStep: The new WdlStep object.
        """

        step = WdlStep(
            self,
            name=name,
            step_number=step_number,
            image=image,
            cpu=cpu,
            memory=memory,
            storage=storage,
            output_dir=self._default_output_dir,
            localize_by=localize_by,
            delocalize_by=delocalize_by,
        )

        if depends_on:
            step.depends_on(depends_on)

        # register the Step
        self._all_steps.append(step)

        return step

    def run_for_each_row(self, table):
        """Run the pipeline in parallel for each row of the given table"""

        super().run_for_each_row(self, table)

    def run(self):
        """Generate WDL"""

        super().run()

        if len(self._all_steps) == 0:
            return

        if len({step.name for step in self._all_steps}) > 1:
            raise ValueError("Conversion of WDL is not yet implemented for pipelines with more than 1 Step")

        step = self._all_steps[0]

        inputs = []
        for input_spec in step._input_specs:
            if not input_spec.name:
                raise ValueError(f"Input name not specified for: {input_spec}. All inputs to {self._pipeline._backend} Steps must have a name.")
            inputs.append(f"File {_to_camel_case(input_spec.name)}")

        for input_value_spec in step._input_value_specs:
            if not input_spec.name:
                raise ValueError(f"Input name not specified for: {input_spec}. All inputs to {self._pipeline._backend} Steps must have a name.")
            inputs += f"{input_value_spec.input_type} {_to_camel_case(input_value_spec.name)}\n"

        outputs = []
        for output_spec in step._output_specs:
            output_spec_name = _to_camel_case(output_spec.name or output_spec.filename)
            outputs.append(f"File {output_spec_name} = \"{output_spec.local_path}\"")

        commands = list(step._commands)

        runtime_attributes = []
        if step._memory:
            runtime_attributes.append(f"memory: {step._memory}")
        if step._cpu:
            runtime_attributes.append(f"cpu: {step._cpu}")
        if step._storage:
            runtime_attributes.append(f"disks: local-disk {step._storage} SSD")

        if step._image:
            runtime_attributes.append(f"docker: {step._image}")

        if step.name:
            task_name = _to_pascal_case(step.name)
        elif step.step_number:
            task_name = f"step{step.step_number}"
        else:
            task_name = "MainTask"

        separator = "\n\t"
        wdl_template_params = {
            "input_section": separator.join(inputs),
            "output_section": separator.join(outputs),
            "commands": separator.join(commands),
            "runtime_section": separator.join(runtime_attributes),
            "task_name": task_name,
            "workflow_name": f"{task_name}Workflow",
        }

        wdl_contents = """
version 1.0

task %(task_name)s {
    input {
        %(input_section)s
    }
    
    command <<< 
        # Set the exit code of a pipeline to that of the rightmost command
        # to exit with a non-zero status, or zero if all commands of the pipeline exit
        set -o pipefail
        # cause a bash script to exit immediately when a command fails
        set -e
        # cause the bash shell to treat unset variables as an error and exit immediately
        set -u
        # echo each line of the script to stdout so we can see what is happening
        set -o xtrace
        #to turn off echo do 'set +o xtrace'

        %(commands)s
    >>>
    
    output {
        %(output_section)s
    }
    
    runtime {
        %(runtime_section)s
    }
}

workflow %(workflow_name)s {
    call %(task_name)s
}        
""" % wdl_template_params

        args = self.parse_args()

        print(f"Writing wdl to {args.wdl_output_path}")
        with open(args.wdl_output_path, "wt") as f:
            f.write(wdl_contents.lstrip())

    def _get_localization_root_dir(self, localize_by):
        """Return the top-level root directory where localized files will be copied"""
        return "/"

    def _transfer_all_steps(self):
        """This method performs the core task of executing a pipeline. It traverses the execution graph (DAG) of
        user-defined Steps and decides which steps can be skipped, and which should be executed (ie. transferred to
        the execution backend).
        """
        pass


class WdlStep(Step):
    """This class contains Hail Batch-specific extensions of the Step class"""

    def __init__(
        self,
        pipeline,
        name=None,
        step_number=None,
        image=None,
        cpu=None,
        memory=None,
        storage=None,
        output_dir=None,
        localize_by=Localize.COPY,
        delocalize_by=Delocalize.COPY,
    ):
        """Step constructor.

        Args:
            pipeline (WdlPipeline): The pipeline that this Step is a part of.
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
                bytes.
            output_dir (str): Optional destination directory to which the local path(s) should be delocalized.
            localize_by (Localize): If specified, this will be the default Localize approach used by Step inputs.
            delocalize_by (Delocalize): If specified, this will be the default Delocalize approach used by Step outputs.
        """
        super().__init__(
            pipeline,
            name,
            step_number=step_number,
            output_dir=output_dir,
            localize_by=localize_by,
            delocalize_by=delocalize_by,
            add_force_command_line_args=False,
            add_skip_command_line_args=False,
        )

        self._image = image
        self._cpu = cpu
        self._memory = memory
        self._storage = storage

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
                optional suffixes are K, Ki, M, Mi, G, Gi, T, Ti, P, and Pi.
        """
        self._storage = storage
        return self

    def _transfer_step(self):
        """Submit this Step to the backend. This method is only called if the Step isn't skipped."""

        pass

    def _get_supported_localize_by_choices(self):
        """Returns the set of Localize options supported by WdlStep"""

        return super()._get_supported_localize_by_choices() | {
            Localize.COPY,
        }

    def _get_supported_delocalize_by_choices(self):
        """Returns the set of Delocalize options supported by WdlStep"""

        return super()._get_supported_delocalize_by_choices() | {
            Delocalize.COPY,
        }

    def _preprocess_input_spec(self, input_spec):
        """This method is called by step.input(..) immediately when the input is first specified, regardless of whether
        the Step runs or not. It validates the input_spec's localize_by value and adds any commands to the
        Step necessary for performing this localization.

        Args:
            input_spec (InputSpec): The input to localize.
        """

        return super()._preprocess_input_spec(input_spec)

    def _transfer_input_spec(self, input_spec):
        """When a Step isn't skipped and is being transferred to the execution backend, this method is called for
        each input to the Step. It performs the Steps necessary for localizing this input.

        Args:
            input_spec (InputSpec): The input to localize.
        """
        super()._transfer_input_spec(input_spec)

    def _preprocess_output_spec(self, output_spec):
        """This method is called by step.output(..) immediately when the output is first specified, regardless of
        whether the Step runs or not. It validates the output_spec.

        Args:
            output_spec (OutputSpec): The output to preprocess.
        """
        if not output_spec.name and not output_spec.filename:
            raise ValueError(f"{output_spec} both name and filename are unspecified")

        super()._preprocess_output_spec(output_spec)

    def _transfer_output_spec(self, output_spec):
        """When a Step isn't skipped and is being transferred to the execution backend, this method is called for
        each output of the Step. It performs the steps necessary to delocalize the output using the approach requested
        by the user via the delocalize_by parameter.

        Args:
            output_spec (OutputSpec): The output to delocalize.
        """
        super()._transfer_output_spec(output_spec)
