# step-pipeline


This library serves as a thin layer on top of Hail Batch (and eventually other execution engines) 
to provide the following features:

- Skip steps that already ran and completed successfully (eg. their output files already exist and are newer than their input files). This is done prior to submitting the pipeline to the execution engine.
- Make it easier to switch between different ways of localizing files (copy, gcsfuse, etc.) with minimal changes to pipeline code
- Automatically define pipeline command-line args to force or skip execution of paticular steps
- Use a config file to store various pipeline parameters (eg. cloud project account, etc.)
- Add commands for sending pipeline completion notifications to Slack
- Add commands for profiling pipeline steps while they are running, and saving this info to external storage 
- Generate a diagram of the pipeline execution graph (DAG)

Another longer-term goal is to allow the same pipeline definition to be submitted to different 
backends with minimal changes - including Hail Batch, Terra/Cromwell, and others. 
This can only work for pipelines that use the subset of workflow definition features that is shared
across the relevant execution engines.  

---

### Installation

To install the `step-pipeline` library, run:
```
python3 -m pip install step-pipeline
```

---

### Docs

[API docs](https://bw2.github.io/step-pipeline/docs/_build/html/index.html)

---