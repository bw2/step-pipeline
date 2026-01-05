step-pipeline
=============

A pipeline framework built on top of `Hail Batch <https://hail.is/docs/batch/index.html>`_
that makes it easy to build, run, and manage computational workflows.

.. code-block:: bash

   pip install git+https://github.com/bw2/step-pipeline.git


Quick Start
-----------

Here's a minimal example that runs a simple command:

.. code-block:: python

   from step_pipeline import pipeline, Backend

   # Create a pipeline
   bp = pipeline(backend=Backend.HAIL_BATCH_SERVICE)
   bp.set_name("my-pipeline")

   # Create a step
   step = bp.new_step("hello world", image="ubuntu:latest", cpu=1)
   step.command("echo 'Hello from Hail Batch!'")

   # Run the pipeline
   bp.run()


Working with Files
------------------

Steps can localize input files and delocalize outputs:

.. code-block:: python

   from step_pipeline import pipeline, Backend, Localize

   bp = pipeline(backend=Backend.HAIL_BATCH_SERVICE)
   step = bp.new_step("process data", image="ubuntu:latest", storage="50G")

   # Localize input file
   local_input = step.input("gs://my-bucket/input.txt", localize_by=Localize.COPY)

   # Run command
   step.command(f"wc -l {local_input} > output.txt")

   # Delocalize output
   step.output("output.txt", output_path="gs://my-bucket/output.txt")

   bp.run()


Key Features
------------

- **Skip completed steps** - Automatically skips steps whose outputs already exist and are newer than inputs
- **Flexible file localization** - Copy, gcsfuse, or hail-hadoop-copy
- **Built-in CLI args** - Force or skip specific steps via command line
- **Multiple backends** - Hail Batch (local/service), Terra, Cromwell
- **Pipeline visualization** - Generate DAG diagrams of your workflow


Key Classes & Functions
-----------------------

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Name
     - Description
   * - :func:`~step_pipeline.main.pipeline`
     - Create a new pipeline instance
   * - :class:`~step_pipeline.pipeline.Pipeline`
     - Main pipeline class for orchestrating steps
   * - :class:`~step_pipeline.pipeline.Step`
     - A single step/job in the pipeline
   * - :class:`~step_pipeline.constants.Backend`
     - Execution backend (HAIL_BATCH_LOCAL, HAIL_BATCH_SERVICE, TERRA, CROMWELL)
   * - :class:`~step_pipeline.io.Localize`
     - How to localize input files (COPY, GSUTIL_COPY, HAIL_HADOOP_COPY, etc.)
   * - :class:`~step_pipeline.io.Delocalize`
     - How to delocalize output files


Backends
--------

step-pipeline supports multiple execution backends:

- **HAIL_BATCH_LOCAL** - Run locally using Docker
- **HAIL_BATCH_SERVICE** - Run on Hail Batch cloud service
- **TERRA** - Generate WDL for Terra
- **CROMWELL** - Generate WDL for Cromwell

.. code-block:: python

   from step_pipeline import pipeline, Backend

   # Local execution
   bp = pipeline(backend=Backend.HAIL_BATCH_LOCAL)

   # Cloud execution via Hail Batch
   bp = pipeline(backend=Backend.HAIL_BATCH_SERVICE)


API Reference
-------------

.. toctree::
   :maxdepth: 2

   source/step_pipeline


Indices
-------

* :ref:`genindex`
* :ref:`modindex`
