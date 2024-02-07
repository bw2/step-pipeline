"""Exapmle step pipeline that converts CRAM/BAM file(s) to FASTQ format."""

import os
import re
from step_pipeline import pipeline, Backend, Localize, Delocalize, files_exist


DOCKER_IMAGE = "weisburd/step-pipeline-examples@sha256:63ec0c8b340547047e98b17f4da33cb42952653301133bfd906c7c6f3710949b"

REFERENCE_FASTA_PATH = {
    "37": "gs://gcp-public-data--broad-references/hg19/v0/Homo_sapiens_assembly19.fasta",
    "38": "gs://gcp-public-data--broad-references/hg38/v0/Homo_sapiens_assembly38.fasta",
    "t2t": "gs://gcp-public-data--broad-references/t2t/v2/chm13v2.0.maskedY.fasta",
}
REFERENCE_FASTA_FAI_PATH = {
    "37": "gs://gcp-public-data--broad-references/hg19/v0/Homo_sapiens_assembly19.fasta.fai",
    "38": "gs://gcp-public-data--broad-references/hg38/v0/Homo_sapiens_assembly38.fasta.fai",
    "t2t": "gs://gcp-public-data--broad-references/t2t/v2/chm13v2.0.maskedY.fasta.fai",
}

def main():
    batch_pipeline = pipeline(backend=Backend.HAIL_BATCH_SERVICE)
    p = batch_pipeline.get_config_arg_parser()
    p.add_argument("-o", "--output-dir", required=True, help="output directory gs:// path")
    p.add_argument("-R", "--reference", choices={"37", "38"}, default="38", help="Reference genome fasta to use for "
                                                                                 "reading CRAM input files.")
    p.add_argument("--dont-sort", action="store_true", help="Prior to FASTQ conversion, the input bam/cram file needs "
        "to be in read name sort order rather than the typical genomic coordinate sort order. If this option is "
        "specified, the pipeline will assume the file is already in read name sort order and will skip the sort step.")
    p.add_argument("bam_or_cram_path", nargs="+", help="CRAM or BAM path(s) to convert to fastq")
    args, _ = p.parse_known_args()

    batch_pipeline.set_name(f"cram_to_fastq: {len(args.bam_or_cram_path)} files")
    for bam_or_cram_path in args.bam_or_cram_path:
        # step 1: sort by queryname
        cpus = 8
        prefix = re.sub(".bam$|.cram$", "", os.path.basename(bam_or_cram_path))
        s1 = batch_pipeline.new_step(f"sort: {prefix}", step_number=1, arg_suffix="step1",
            image=DOCKER_IMAGE, cpu=cpus, memory="highmem", storage="200G", output_dir=args.output_dir)
        s1.switch_gcloud_auth_to_user_account()

        local_bam_or_cram = s1.input(bam_or_cram_path, localize_by=Localize.GSUTIL_COPY)
        output_filename_r1 = f"{prefix}.R1.fastq.gz"
        output_filename_r2 = f"{prefix}.R2.fastq.gz"

        reference_arg = ""
        if args.reference:
            local_reference_fasta = s1.input(REFERENCE_FASTA_PATH[args.reference], localize_by=Localize.HAIL_BATCH_CLOUDFUSE)
            reference_arg = f"--reference {local_reference_fasta}"

        s1.command("set -ex")
        s1.command("cd /io")
        s1.command(f"time samtools sort {reference_arg} -n --threads {max(1, cpus)} -m 3G -o {prefix}.sorted.cram {local_bam_or_cram} ")
        s1.output(f"{prefix}.sorted.cram")

        # step 2: convert to fastq
        cpus = 1
        s2 = batch_pipeline.new_step(f"fastq: {prefix}", step_number=2, arg_suffix="step2",
            image=DOCKER_IMAGE, cpu=cpus, memory="standard", storage="300G", output_dir=args.output_dir)
        s2.switch_gcloud_auth_to_user_account()

        local_bam_or_cram = s2.use_previous_step_outputs_as_inputs(s1)
        output_filename_r1 = f"{prefix}.R1.fastq.gz"
        output_filename_r2 = f"{prefix}.R2.fastq.gz"

        reference_arg = ""
        if args.reference:
            local_reference_fasta = s2.input(REFERENCE_FASTA_PATH[args.reference], localize_by=Localize.HAIL_BATCH_CLOUDFUSE)
            reference_arg = f"--reference {local_reference_fasta}"

        s2.command("set -ex")
        s2.command("cd /io")
        s2.command(f"time samtools fastq {reference_arg} -1 {output_filename_r1} -2 {output_filename_r2} {local_bam_or_cram}")
        s2.output(output_filename_r1)
        s2.output(output_filename_r2)

        # step 3: align to T2T
        cpus = 8
        s3 = batch_pipeline.new_step(f"bwa mem: {prefix}", step_number=3, arg_suffix="step3",
            image=DOCKER_IMAGE, cpu=cpus, memory="standard", storage="300G", output_dir=args.output_dir)
        s3.switch_gcloud_auth_to_user_account()
        local_r1_fastq, local_r2_fastq = s3.use_previous_step_outputs_as_inputs(s2, localize_by=Localize.COPY)

        local_reference_fasta, _, _, _, _, _, _ = s3.inputs([
            "gs://gcp-public-data--broad-references/t2t/v2/chm13v2.0.maskedY.fasta",
            "gs://gcp-public-data--broad-references/t2t/v2/chm13v2.0.maskedY.fasta.fai",
            "gs://gcp-public-data--broad-references/t2t/v2/chm13v2.0.maskedY.fasta.amb",
            "gs://gcp-public-data--broad-references/t2t/v2/chm13v2.0.maskedY.fasta.ann",
            "gs://gcp-public-data--broad-references/t2t/v2/chm13v2.0.maskedY.fasta.bwt",
            "gs://gcp-public-data--broad-references/t2t/v2/chm13v2.0.maskedY.fasta.pac",
            "gs://gcp-public-data--broad-references/t2t/v2/chm13v2.0.maskedY.fasta.sa",
        ], localize_by=Localize.COPY)

        s3.command("set -ex")
        s3.command("cd /io")
        s3.command(f"bwa mem -t {cpus} {local_reference_fasta} {local_r1_fastq} {local_r2_fastq} | "
                   f"samtools sort --reference {local_reference_fasta} -o {prefix}.t2t.cram - ")
        s3.command(f"samtools index {prefix}.t2t.cram")
        #s3.command(f"samtools view -c {prefix}.t2t.cram")

        s3.output(f"{prefix}.t2t.cram")
        s3.output(f"{prefix}.t2t.cram.crai")

        print(f"Output: {prefix}.t2t.cram")

    batch_pipeline.run()

if __name__ == "__main__":
    main()

