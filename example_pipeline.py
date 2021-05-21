import os

from step_pipeline import step_pipeline, ExecutionEngine

with step_pipeline("print index pipeline", execution_engine=ExecutionEngine.HAIL_BATCH) as bp:
    bp.set_default_image("weisburd/base-image:latest")

    s1 = bp.new_step("step1")
    #s1.command(f"cd {os.getcwd()}")
    s1.post_to_slack("start")
    s1.command("pwd")
    s1.command("ls -l")
    s1.command("echo yes! > temp.txt")
    s1.post_to_slack("end")

    #s1.input("gs://")
    #s1.output_dir("gs://")