from step_pipeline import pipeline, Backend

sp = pipeline("summarize fasta index", backend=Backend.HAIL_BATCH_SERVICE)

s1 = sp.new_step("save HLA contigs")
ref_fasta_index = s1.input(
    "gs://gcp-public-data--broad-references/hg38/v0/Homo_sapiens_assembly38.fasta.fai")

s1.command(f"cat {ref_fasta_index} | grep HLA- > hg38.HLA_contigs")

s1.output("hg38.HLA_contigs", output_dir="gs://seqr-bw/step-pipeline-test/intergration_test1")

sp.run()

