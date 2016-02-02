#! /bin/bash

# This should resolve to something like /datastore/MELA-0005_files/a76d7d7a-6f19-4ae9-a152-7b909130946c/ 
# Which contains all the VCFs for a specific workflow.
PATH_TO_VCFs=$0

# These should be the file names, such as 35a74e53-16ff-4764-8397-6a9b02dfe733.broad-mutect.20151216.somatic.snv_mnv or sorted_indel.bcftools-norm.
# File suffixes will be added in by the script.
SNV_FILE=$1
INDEL_FILE=$2
SV_FILE=$3

WORKFLOW=$4

echo "unzipping..."
for f in $(ls $PATH_TO_VCFs/*.vcf.gz) ; do
        bgzip -f -d $f
done;

mkdir ~/vcflib
wget https://raw.githubusercontent.com/vcftools/vcftools/v0.1.14/src/perl/Vcf.pm -O ~/vcflib/Vcf.pm

sudo docker run --rm \
        -v $PATH_TO_VCFs/:/VCFs/ \
        -v ~/vcflib/:/home/ngseasy/vcflib/ \
         -v /datastore/refdata/public:/ref \
         compbio/ngseasy-base:a1.0-002 /bin/bash -c "echo \"validating inputs...\" ; \
				( vcf-validator /VCFs/$SNV_FILE.vcf 2>&1 ) > /VCFs/snv.validation.log ; \
				( vcf-validator /VCFs/$INDEL_FILE.vcf 2>&1 ) > /VCFs/indel.validation.log ; \
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

# ##INFO=<ID=SCTG,Number=1,Type=String,Description="SCTG - manually added to header">
# ##INFO=<ID=INSERTION,Number=1,Type=String,Description="INSERTION - manually added to header">
# ##INFO=<ID=EVDNC,Number=1,Type=String,Description="EVDNC - manually added to header">
# ##INFO=<ID=NDISC,Number=1,Type=String,Description="NDISC - manually added to header">
# ##INFO=<ID=PONCOUNT,Number=1,Type=String,Description="PONCOUNT - manually added to header">
# ##INFO=<ID=TDISC,Number=1,Type=String,Description="TDISC - manually added to header">
if [[ $WORKFLOW == "BROAD" ]] ; then
	bgzip -c -d $PATH_TO_VCFs/combined_sorted_padded.vcf.gz > $PATH_TO_VCFs/combined_sorted_padded.vcf
	# TODO: Inject extra headers.
	bgzip -f $PATH_TO_VCFs/combined_sorted_padded.vcf
	tabix -p vcf $PATH_TO_VCFs/combined_sorted_padded.vcf.gz
fi


echo "done!" 
