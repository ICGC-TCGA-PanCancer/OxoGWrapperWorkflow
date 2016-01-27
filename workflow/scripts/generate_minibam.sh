#! /bin/bash

BAMTYPE=$1
BAMPATH=$2
RULESPATH=$3
VCFSPATH=$4
OUTDIR=$5

sudo docker run --rm --name="bare_variantbam_call_${BAMTYPE}" \
                -v $VCFSPATH:/datafiles/:rw \
                -v $RULESPATH:/rules.txt \
                -v $BAMPATH:/input.bam \
                -v $OUTDIR:/outdir/:rw \
        oxog /bin/bash -c "/cga/fh/pcawg_pipeline/modules/VariantBam/variant \
        -o /outdir/${BAMTYPE}_minibam.bam \
        -i /input.bam \
        -r /rules.txt  \
        -l /datafiles/sv.clean.sorted.vcf \
        -l /datafiles/snv.clean.sorted.vcf \
        -l /datafiles/indel.clean.sorted.vcf \
        2>&1 > /outdir/variantbam_${BAMTYPE}.log "
