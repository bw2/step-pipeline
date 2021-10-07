from step_pipeline import batch_pipeline, LocalizationStrategy, BatchBackend, DelocalizationStrategy

with batch_pipeline("summarize fasta index", backend=BatchBackend.SERVICE) as bp:
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

