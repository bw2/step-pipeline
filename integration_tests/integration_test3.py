import re
from step_pipeline import pipeline, Localize, Delocalize, Backend

with pipeline("summarize fasta index", backend=Backend.HAIL_BATCH_SERVICE) as sp:
    sp.default_output_dir("gs://seqr-bw/step-pipeline-test/intergration_test3")

    p = sp.get_config_arg_parser()
    p.add_argument("--use-cloudfuse", action="store_true", help="Use CLOUDFUSE to access the reference index file")
    args = sp.parse_args()

    # step 1
    s1 = sp.new_step("save HLA contigs", step_number=1)
    ref_fasta_index = s1.input(
        "gs://gcp-public-data--broad-references/hg38/v0/Homo_sapiens_assembly38.fasta.fai",
        localize_by=Localize.HAIL_BATCH_CLOUDFUSE if args.use_cloudfuse else Localize.COPY,
    )

    output_filename = re.sub(".fasta.fai$", "", ref_fasta_index.filename) + ".HLA_contigs"
    s1.command("set -ex")
    s1.command(f"cat {ref_fasta_index} | grep HLA- > {output_filename}")
    s1.output(output_filename, delocalize_by=Delocalize.COPY)

    # step 2
    s2 = sp.new_step("count HLA contigs", step_number=2)
    s2.switch_gcloud_auth_to_user_account()
    input_spec = s2.use_previous_step_outputs_as_inputs(s1, localize_by=Localize.GSUTIL_COPY)

    s2.command("set -ex")
    s2.command("echo Number of HLA contigs:")
    s2.command(f"cat {input_spec} | wc -l > num_hla_contigs.txt")
    s2.output("num_hla_contigs.txt")
    s2.post_to_slack("step2 is done")

