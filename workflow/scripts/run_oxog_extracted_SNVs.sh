#! /bin/bash

VCF1=$1
VCF2=$2
VCF3=$3

BAM1=$4
BAM2=$5

ALIQUOTID=$6

OXOQSCORE=$7

NUMINDELS1=$(grep "^[^#]" $VCF1  | wc -l)
echo "$VCF1 has $NUMINDELS1 INDELS"
NUMINDELS2=$(grep "^[^#]" $VCF2  | wc -l)
echo "$VCF2 has $NUMINDELS2 INDELS"
NUMINDELS3=$(grep "^[^#]" $VCF3  | wc -l)
echo "$VCF3 has $NUMINDELS3 INDELS"

VCFFORDOCKER=""
VCFFOROXOG=""
if (( NUMINDELS1 > 0 )) ; then
        VCFFORDOCKER+=" -v ${VCF1}:/VCF1.vcf.gz "
        VCFFOROXOG+=" /VCF1.vcf.gz "
fi
if (( NUMINDELS2 > 0 )) ; then
        VCFFORDOCKER+=" -v ${VCF2}:/VCF2.vcf.gz "
        VCFFOROXOG+=" /VCF2.vcf.gz "
fi
if (( NUMINDELS3 > 0 )) ; then
        VCFFORDOCKER+=" -v ${VCF2}:/VCF3.vcf.gz "
        VCFFOROXOG+=" /VCF3.vcf.gz "
fi

if [[ $VCFFORDOCKER != "" ]] ; then

	echo "mount-options for VCF files snippet:  $VCFFORDOCKER"
	echo "VCF files for OxoG: $VCFFOROXOG "


    sudo docker run --rm --name="oxog_container_snv_from_indel" \
                -v /datastore/refdata/:/cga/fh/pcawg_pipeline/refdata/ \
                -v /datastore/oncotator_db/:/cga/fh/pcawg_pipeline/refdata/public/oncotator_db/ \
                -v /datastore/oxog_workspace_extracted_snvs/:/cga/fh/pcawg_pipeline/jobResults_pipette/jobs/${ALIQUOTID}/:rw \
                -v /datastore/bam/:/datafiles/BAM/ \
            	${VCFFORDOCKER} \
            	-v /datastore/oxog_results_extracted_snvs/:/cga/fh/pcawg_pipeline/jobResults_pipette/results:rw \
          oxog /cga/fh/pcawg_pipeline/pipelines/run_one_pipeline.bash pcawg /cga/fh/pcawg_pipeline/pipelines/oxog_pipeline.py \
                ${ALIQUOTID} \
                /datafiles/BAM/${BAM1} \
                /datafiles/BAM/${BAM2} \
                ${OXOQSCORE} \
            	${VCFFOROXOG}
else
	echo "There were NO SNVs extracted from INDELs to run OxoG on."
fi
