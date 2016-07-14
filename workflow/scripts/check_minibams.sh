#! /bin/bash

# Inputs: space-separate list of absolute paths to pass-filtered SNVs (except for MuSE
# which is not pass-filtered, so include the original; and smufin, which will only have
# an INDEL that should be included).   

if [ "$#" -eq 0 ] ; then
	echo "ERROR: No arguments given!"
	exit 1
fi

# Store the inputs in an array.
VCFS=( "$@" )

echo "Arguments given: $@"

for vcf in ${VCFS[@]}; do
	echo "----------------------------------------------------------------"
	echo "Checking file: $vcf"
	OUTFILE=$(basename $vcf)
	OUTFILE=${OUTFILE/\.vcf\.gz/.chr22.positions.txt}
	
	# Working just with Chromosome 22 - this it not a comprehensive in-depth reconciliation, just a quick sanitcy-check that should catch most problems. 
	zcat $vcf | grep ^22 | cut -f2 > /datastore/vcf/$OUTFILE

	while read location; do
		echo "for location $location:"
		# Get the count in the normal bam for this location, using samtools
		PATH_TO_NORMAL=$( ( [ -f /datastore/bam/normal/*/*.bam ] && echo /datastore/bam/normal/*/*.bam) || ([ -f /datastore/bam/normal/*.bam ] && echo /datastore/bam/normal/*.bam))
		COUNT_IN_NORMAL=$(samtools view $PATH_TO_NORMAL 22:$location-$location -c)
		echo "count in normal - original bam: $COUNT_IN_NORMAL"
	
		# Get count in normal minibam, using samtools
		NORMAL_FILE_BASENAME=$(basename /datastore/bam/normal/*/*.bam)
		COUNT_IN_NORMAL_MINIBAM=$(samtools view /datastore/variantbam_results/${NORMAL_FILE_BASENAME/\.bam/_minibam.bam} 22:$location-$location -c)
		echo "count in normal - minibam: $COUNT_IN_NORMAL_MINIBAM"
	
		if [ "$COUNT_IN_NORMAL" != "$COUNT_IN_NORMAL_MINIBAM" ] ; then
			echo "MISMATCH in normal original vs normal minibam ! Something may have gone wrong in vcf merge or in variantbam!"
			exit 1;
		fi
		# for each tumour, get count in original and mini BAMs. 
		for tumour in $(ls /datastore/bam/tumour/*/*.bam /datastore/bam/tumour/*.bam) ; do 
			TUMOUR_FILE_BASENAME=$(basename $tumour)
			COUNT_IN_TUMOUR_1=$(samtools view $tumour 22:$location-$location -c)
			echo "count in tumour ${TUMOUR_FILE_BASENAME}: $COUNT_IN_TUMOUR_1"
			COUNT_IN_TUMOUR_1_MINIBAM=$(samtools view /datastore/variantbam_results/${TUMOUR_FILE_BASENAME/\.bam/_minibam.bam} 22:$location-$location -c)
			echo "count in tumour ${TUMOUR_FILE_BASENAME/\.bam/_minibam.bam}: $COUNT_IN_TUMOUR_1_MINIBAM"
			if [ "$COUNT_IN_TUMOUR_1" != "$COUNT_IN_TUMOUR_1_MINIBAM" ] ; then
				echo "MISMATCH in tumour original vs tumour minibam! Something may have gone wrong in vcf merge or in variantbam!"
				exit 1;
			fi
		done
	done </datastore/vcf/$OUTFILE
done
