from step_pipeline import batch_pipeline

with batch_pipeline("summarize fasta index") as bp:
    #bp.default_image("weisburd/base-image:latest")

    s1 = bp.new_step("step1")
    #s1.command(f"cd {os.getcwd()}")
    #s1.post_to_slack("start")
    s1.command("echo hello")
    s1.command("pwd")
    #s1.command(f"cat {input_spec.get_local_path()}")
    #s1.command("ls -l")
    #s1.command("echo yes! > temp.txt")
    #s1.post_to_slack("end")

    #s1.input("gs://")
    #s1.output_dir("gs://")


