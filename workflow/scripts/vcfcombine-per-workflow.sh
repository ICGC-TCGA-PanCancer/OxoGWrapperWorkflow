#! /bin/bash

# This should resolve to something like /datastore/MELA-0005_files/a76d7d7a-6f19-4ae9-a152-7b909130946c/ 
# Which contains all the VCFs for a specific workflow.
PATH_TO_VCFs=$0

# These should be the file names, such as 35a74e53-16ff-4764-8397-6a9b02dfe733.broad-mutect.20151216.somatic.snv_mnv or sorted_indel.bcftools-norm.
# File suffixes will be added in by the script.
SNV_FILE=$1
INDEL_FILE=$2
SV_FILE=$3

echo "unzipping..."
for f in $(ls $PATH_TO_VCFs/*.vcf.gz) ; do
        bgzip -f -d $f
done;

sudo docker run --rm \
        -v $PATH_TO_VCFs/:/VCFs/ \
         -v /datastore/refdata/public:/ref \
         compbio/ngseasy-base:a1.0-002 /bin/bash -c "echo \"validating inputs...\" ; \
                vcfcheck -f /ref/Homo_sapiens_assembly19.fasta /VCFs/$SNV_FILE.vcf > /VCFs/snv.validation.log ; \
                vcfcheck -f /ref/Homo_sapiens_assembly19.fasta /VCFs/$INDEL_FILE.vcf > /VCFs/normalized_sorted_indel.validation.log ; \
                echo \"combining VCFs...\" ; \
                vcfcombine /VCFs/$SNV_FILE.vcf \
                        /VCFs/$INDEL_FILE.vcf \
                > /VCFs/combined.vcf ; \
                echo \"sorting combined VCF...\" ; \
                vcf-sort /VCFs/combined.vcf > /VCFs/combined_sorted.vcf ; \
                echo \"validating combined/sorted VCF...\" ; \
                vcfcheck -f /ref/Homo_sapiens_assembly19.fasta /VCFs/combined_sorted.vcf > /VCFs/combined_sorted_VCF.validation.log ; \
                echo \"zipping... \" ; \
                bgzip -c -f /VCFs/combined_sorted.vcf > /VCFs/combined_sorted.vcf.gz \
                echo \"indexing...\" ; \
                tabix -p vcf /VCFs/combined_sorted.vcf.gz "

echo "padding combined vcf..."

python3 pad_vcf.py $PATH_TO_VCFs/combined_sorted.vcf > $PATH_TO_VCFs/combined_sorted_padded.vcf
bgzip -f $PATH_TO_VCFs/combined_sorted_padded.vcf
tabix -p vcf $PATH_TO_VCFs/combined_sorted_padded.vcf.gz

echo "done!" 
