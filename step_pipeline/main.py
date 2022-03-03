"""This module contains the user-facing interface of the step_pipeline library"""

import configargparse

from .constants import Backend
from .batch import _BatchPipeline

def pipeline(name=None, backend=Backend.HAIL_BATCH_SERVICE, config_file_path="~/.step_pipeline"):
    """Creates a pipeline object.

    Usage:
        with step_pipeline("my pipeline") as sp:
            s = sp.new_step(..)
            ... step definitions ...

        or

        sp = step_pipeline("my pipeline")
        s = sp.new_step(..)
        ...
        sp.run()

    Args:
        name (str): Pipeline name.
        backend (Backend): The backend to use for executing the pipeline.
        config_file_path (str): path of a configargparse config file.

    Return:
        Pipeline object that can be used to create Steps and then execute the pipeline.
    """

    config_arg_parser = configargparse.ArgumentParser(
        add_config_file_help=True,
        add_env_var_help=True,
        formatter_class=configargparse.HelpFormatter,
        default_config_files=[config_file_path],
        ignore_unknown_config_file_keys=True,
        config_file_parser_class=configargparse.YAMLConfigFileParser,
    )

    config_arg_parser.add_argument("--backend", help="The backend system to use for executing the pipeline.",
        default=Backend.HAIL_BATCH_SERVICE.name, choices=[name for e in Backend for name in [e.name, e.value]])

    args, _ = config_arg_parser.parse_known_args(ignore_help_args=True)

    # create and yield the pipeline
    backend = Backend[args.backend] if args.backend else backend
    if backend in (Backend.HAIL_BATCH_SERVICE, Backend.HAIL_BATCH_LOCAL):
        pipeline = _BatchPipeline(name=name, config_arg_parser=config_arg_parser, backend=backend)
    else:
        raise ValueError(f"Unknown backend: {args.backend}")

    return pipeline