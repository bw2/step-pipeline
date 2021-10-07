from step_pipeline import batch_pipeline, LocalizationStrategy, BatchBackend, DelocalizationStrategy

bp = batch_pipeline("summarize fasta index", backend=BatchBackend.SERVICE)
s1 = bp.new_step("step1: save HLA contigs")

input_spec = s1.input(
    "gs://gcp-public-data--broad-references/hg38/v0/Homo_sapiens_assembly38.fasta.fai",
    localization_strategy=LocalizationStrategy.COPY,   # LocalizationStrategy.HAIL_BATCH_GCSFUSE
)

output_filename = f"{input_spec.get_filename().replace('.fasta.fai', '')}.HLA_contigs"
s1.command(f"cat {input_spec.get_local_path()} | grep HLA- > {output_filename}")
s1.command("ls")

args = bp.parse_args()
s1.output_dir(f"gs://{args.batch_temp_bucket}/tmp")
s1.output(output_filename, delocalization_strategy=DelocalizationStrategy.COPY)

s2 = bp.new_step("step2: count HLA contigs")
input_specs = s2.use_previous_step_outputs_as_inputs(s1, localization_strategy=LocalizationStrategy.COPY)
s2.command("echo Number of HLA contigs:")
s2.command(f"cat {input_specs[0].get_local_path()} | wc -l")

result = bp.run()


#%%

s1.switch_gcloud_auth_to_user_account()
s1.post_to_slack("start")

# TODO test local + remote and bash + python

