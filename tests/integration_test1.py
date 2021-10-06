"""

# PythonJob using ServiceBackend
b = hb.Batch(backend=hb.ServiceBackend(billing_project="tgg-rare-disease", bucket="macarthurlab-cromwell"), requester_pays_project="bw2-rare-disease", default_python_image='hailgenetics/hail:0.2.77')
def multiply():
      return 15
middle = b.new_python_job()
r_mult = middle.call(multiply)
res = b.run()


res.status()
res.get_job(1)
res.get_job_log(1)


# BashJob using ServiceBackend
b = hb.Batch(backend=hb.ServiceBackend(billing_project="tgg-rare-disease", bucket="macarthurlab-cromwell"), requester_pays_project="bw2-rare-disease", default_python_image='hailgenetics/hail:0.2.77')
def multiply():
      return 15
j = b.new_job()
j.command("echo hello")
res = b.run()



# BashJob using LocalBackend
b = hb.Batch(backend=hb.LocalBackend(), requester_pays_project="bw2-rare-disease", default_python_image='hailgenetics/hail:0.2.77')
def multiply():
      return 15
j = b.new_job()
j.command("echo hello")
res = b.run()
"""

#%%

from step_pipeline import batch_pipeline, LocalizationStrategy, BatchBackend
bp = batch_pipeline("summarize fasta index", backend=BatchBackend.LOCAL)
args = bp.parse_args()

s1 = bp.new_step("step1")
s1.storage(5)
input_spec = s1.input("gs://gcp-public-data--broad-references/hg38/v0/Homo_sapiens_assembly38.fasta.fai", localization_strategy=LocalizationStrategy.COPY)  # LocalizationStrategy.HAIL_BATCH_GCSFUSE

#s1.post_to_slack("start")
output_filename = f"{input_spec.get_filename().replace('.fasta.fai', '')}.num_chroms"
s1.command("pwd")
#s1.command(f"cat {input_spec.get_local_path()} | wc -l > {output_filename}")
#s1.output_dir(f"{args.batch_temp_bucket}/tmp")
#s1.output(output_filename)


result = bp.run()


#%%
# TODO test local + remote and bash + python

#s1.command("ls -l")
#s1.command("echo yes! > temp.txt")
#s1.post_to_slack("end")

#s1.input("gs://")
#s1.output_dir("gs://")

