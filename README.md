# step-pipeline

Python library for defining and executing pipelines that run in container or VM execution environments like 
Hail Batch, Terra, SGE, etc., and that localize/delocalize input/output files to cloud storage services like 
Google Storage buckets. Currently, it supports Hail Batch and Google Cloud Storage only.  

The main goal of this library is to reduce repetitive code in your python pipeline definition script (PPDS) by 
taking care of common pipeline needs such as skipping execution steps that don't need to run because their 
output files already exist.

The things it takes care of include: 
- before submitting your pipeline for execution, it  
  a) checks pipeline input files and throws an error if any are missing.  
  b) skips steps whose outputs already exist and are newer than the inputs. 
- adds command-line args to your PPDS for skipping some steps and/or forcing re-execution of others
- provides a simplified API for localizing input files and delocalizing output files using different strategies 
  (copy, gcfuse, etc.)
- notifies you via slack or email when the pipeline completes
- optionally, records profiling info by starting a background process within the job execution container to record cpu 
  and memory at regular intervals while your commands are running
- optionally, generates an image of the pipeline DAG

NOTE: some features only work if specific tools are installed inside the docker container or local environment that's
executing your pipeline.

---

### Installation

To install the `step-pipeline` library, run:
```
python3 -m pip install step-pipeline
```


