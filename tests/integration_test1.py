from step_pipeline import batch_pipeline, LocalizationStrategy, BatchBackend

bp = batch_pipeline("summarize fasta index", backend=BatchBackend.LOCAL)
s1 = bp.new_step("step1")

input_spec = s1.input(
    "gs://gcp-public-data--broad-references/hg38/v0/Homo_sapiens_assembly38.fasta.fai",
    localization_strategy=LocalizationStrategy.COPY,   # LocalizationStrategy.HAIL_BATCH_GCSFUSE
)

output_filename = f"{input_spec.get_filename().replace('.fasta.fai', '')}.num_chroms"
s1.command(f"cat {input_spec.get_local_path()} | wc -l > {output_filename}")
s1.command("ls")

args = bp.parse_args()
s1.output_dir(f"gs://{args.batch_temp_bucket}/tmp")
s1.output(output_filename)

result = bp.run()


#%%
# TODO test local + remote and bash + python

