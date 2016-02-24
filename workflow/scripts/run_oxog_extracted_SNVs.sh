#! /bin/bash
set -e
VCF1=$1
VCF2=$2
VCF3=$3
# MAKE SURE THAT TUMOUR IS BAM1!
BAM1=$4
BAM2=$5

ALIQUOTID=$6

OXOQSCORE=$7

NUMINDELS1=$(zcat $VCF1 | grep "^[^#]" | wc -l)
echo "$VCF1 has $NUMINDELS1 INDELS"
NUMINDELS2=$(zcat $VCF2 | grep "^[^#]" | wc -l)
echo "$VCF2 has $NUMINDELS2 INDELS"
NUMINDELS3=$(zcat $VCF3 | grep "^[^#]" | wc -l)
echo "$VCF3 has $NUMINDELS3 INDELS"

VCFFORDOCKER=""
VCFFOROXOG=""
if (( NUMINDELS1 > 0 )) ; then
        VCFFORDOCKER=" ${VCFFORDOCKER} -v ${VCF1}:/VCF1.vcf.gz "
        VCFFOROXOG=" ${VCFFOROXOG} /VCF1.vcf.gz "
fi
if (( NUMINDELS2 > 0 )) ; then
        VCFFORDOCKER=" ${VCFFORDOCKER} -v ${VCF2}:/VCF2.vcf.gz "
        VCFFOROXOG=" ${VCFFOROXOG} /VCF2.vcf.gz "
fi
if (( NUMINDELS3 > 0 )) ; then
        VCFFORDOCKER=" ${VCFFORDOCKER} -v ${VCF3}:/VCF3.vcf.gz "
        VCFFOROXOG=" ${VCFFOROXOG} /VCF3.vcf.gz "
fi

if [[ $VCFFORDOCKER != "" ]] ; then

	echo "mount-options for VCF files snippet:  $VCFFORDOCKER"
	echo "VCF files for OxoG: $VCFFOROXOG "

	set -x
    docker run --rm --name="oxog_container_snv_from_indel" \
                -v /refdata/:/cga/fh/pcawg_pipeline/refdata/ \
                -v /datastore/oxog_workspace_extracted_snvs/:/cga/fh/pcawg_pipeline/jobResults_pipette/jobs/${ALIQUOTID}/:rw \
                -v /datastore/bam/:/datafiles/BAM/ \
            	${VCFFORDOCKER} \
            	-v /datastore/oxog_results_extracted_snvs/:/cga/fh/pcawg_pipeline/jobResults_pipette/results:rw \
        oxog /bin/bash -c "/cga/fh/pcawg_pipeline/pipelines/run_one_pipeline.bash pcawg /cga/fh/pcawg_pipeline/pipelines/oxog_pipeline.py \
                ${ALIQUOTID} \
                /datafiles/BAM/${BAM1} \
                /datafiles/BAM/${BAM2} \
                ${OXOQSCORE} \
            	${VCFFOROXOG} "
	set +x
	
    # extract results from tarball
	cd /datastore/oxog_results_extracted_snvs && tar -xkfv ./${ALIQUOTID}.gnos_files.tar 
	[ -d /datastore/files_for_upload/snvs_from_indels ] || sudo mkdir -p /datastore/files_for_upload/snvs_from_indels
	# copy resutls to upload dir 
	cp /datastore/oxog_results_extracted_snvs/cga/fh/pcawg_pipeline/jobResults_pipette/jobs/${ALIQUOTID}/links_for_gnos/annotate_failed_sites_to_vcfs/*.vcf.* /datastore/files_for_upload/snvs_from_indels/
	cp /datastore/oxog_results_extracted_snvs/${ALIQUOTID}.gnos_files.tar /datastore/files_for_upload/snvs_from_indels/${ALIQUOTID}.snvs_from_indels.gnos_files.tar
else
	echo "There were NO SNVs extracted from INDELs to run OxoG on."
fi
set +e