import hailtop.batch as hb

b = hb.Batch(name="summarize fasta index", backend=hb.ServiceBackend("tgg-rare-disease",
        remote_tmpdir="gs://gnomad-bw2-delete-after-15-days"))

j = b.new_job(name="save HLA contigs")
ref_fasta_index = b.read_input(
    "gs://gcp-public-data--broad-references/hg38/v0/Homo_sapiens_assembly38.fasta.fai")

j.command(f"cat {ref_fasta_index} | grep HLA- > {j.hla_contigs}")

b.write_output(j.hla_contigs, "gs://seqr-bw/step-pipeline-test/intergration_test1/hg38.HLA_contigs")

b.run()
