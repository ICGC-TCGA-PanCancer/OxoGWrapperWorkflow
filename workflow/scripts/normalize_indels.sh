#! /bin/bash

# This should resolve to something like /datastore/MELA-0005_files/a76d7d7a-6f19-4ae9-a152-7b909130946c/ 
# Which contains all the VCFs for a specific workflow.
PATH_TO_VCFs=$0

# The *name* only of the INDEL file to process. example: 35a74e53-16ff-4764-8397-6a9b02dfe733.broad-snowman.20151216.somatic.indel
# The script will then assume the suffix ".vcf.gz" for a *full* file name: 35a74e53-16ff-4764-8397-6a9b02dfe733.broad-snowman.20151216.somatic.indel.vcf.gz
INDEL_FILE=$1

# This is only necessasry for Broad...
# IF BROAD:
echo "fixing broad indels..."
# broad indel files seem to be missing dots for empty fields, so they need to be fixed...
bgzip -d -c $PATH_TO_VCFs/$INDEL_FILE.vcf.gz  \
        |  sed -e s/"$(echo -e '\t\t')"/"$(echo -e '\t')"."$(echo -e '\t')"./g -e s/"$(echo -e '\t')"$/"$(echo -e '\t')"./g  \
        > $PATH_TO_VCFs/$INDEL_FILE.fixed.vcf
# zip the fixed result.
bgzip -f $PATH_TO_VCFs/$INDEL_FILE.fixed.vcf
# FI
# Processing other workflow can pick up here: 
echo "processing indel VCF"
sudo docker run --rm \
        -v $PATH_TO_VCFs/$INDEL_FILE.fixed.vcf.gz:/input:rw \
        -v /datastore/refdata/public:/ref \
        -v $PATH_TO_VCFs/:/outdir/:rw \
        compbio/ngseasy-base:a1.0-002 /bin/bash -c "echo \"checking...\" ; \
        vcfcheck -f /ref/Homo_sapiens_assembly19.fasta /input > /outdir/sorted_indel.validation.log ; \
        echo \"normalizing...\" ; \
        bcftools norm -cw -m -any -Oz -f /ref/Homo_sapiens_assembly19.fasta /input > /outdir/indel.bcftools-norm.vcf.gz ; \
        echo \"sorting...\" ; \
        vcf-sort /outdir/indel.bcftools-norm.vcf.gz > /outdir/sorted_indel.bcftools-norm.vcf; \
        echo \"indexing...\" ; \
        bgzip -f /outdir/sorted_indel.bcftools-norm.vcf ; \
        tabix -p vcf /outdir/sorted_indel.bcftools-norm.vcf.gz ; "
