cd docs
sphinx-apidoc -o ./source ../step_pipeline/batch.py ../step_pipeline/wdl.py ../step_pipeline/pipeline.py ../step_pipeline/constants.py ../step_pipeline/io.py ../step_pipeline/main.py
make html
