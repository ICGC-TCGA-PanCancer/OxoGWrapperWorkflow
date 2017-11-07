#! /bin/bash

# User should configure these:
gnosMetadataUploadURL="https://gtrepo-osdc-tcga.annailabs.com"
snvPadding="10"
svPadding="500"
indelPadding="200"
studyRefNameOverride="tcga_pancancer_vcf"

gitMoveTestMode="true"
storageSource="aws"
skipDownload="true"
skipUpload="false"
refFile="public/Homo_sapiens_assembly19.fasta"
uploadKey="/datastore/credentials/rsync.key"
gnosKey="/datastore/credentials/gnos.key"
skipOxoG="false"
skipVariantBam="false"
skipAnnotation="false"
uploadURL="oicr@192.170.233.206:~/incoming/bulk_upload/sshorser" 
JSONrepo="git@github.com:ICGC-TCGA-PanCancer/oxog-ops.git"
JSONrepoName="oxog-ops"
JSONfolderName="oxog-aws-jobs-test"
JSONlocation="/home/seqware/gitroot/"
GITemail="denis.yuen+icgc@gmail.com"
GITname="icgc-bot"
GITPemFile="/datastore/credentials/git.pem"
vcfDownloadMethod="gtdownload"
smufinDownloadMethod="filesystemCopy"
bamDownloadMethod="gtdownload"
allowMissingFiles="true"
smufinDownloadMethod="filesystemCopy"
fileSystemSourcePath="/datastore/smufin_files/"
skipBamUpload="false"
skipVcfUpload="true"
gtDownloadVcfKey="/path/to/key"
gtDownloadBamKey="/path/to/key"

PATH_TO_JSON_DIR=$1
PATH_TO_OXOG_CLASSES=$2
DEST_DIR=$3

function update_or_append
{
	echo "searching in $1 for $2 and replacing with $3"
    file=$1
	search_string=$2
	replace_string=$3
	append_string="$search_string=$replace_string"

	if grep -q $search_string $file ; then
		sed -i.bak s/$search_string\ *=.*/$search_string=${replace_string//\//\\\/}/g $file
	else
		echo "$append_string" >> $file
	fi
}

for f in $(ls $PATH_TO_JSON_DIR) ; do
	result=$(java -cp $PATH_TO_OXOG_CLASSES/bin:$PATH_TO_OXOG_CLASSES/classes com.github.seqware.INIGenerator $PATH_TO_JSON_DIR/$f)

	#extract filename from output:
	ini_file_name=$(echo $result | sed 's/.*: \.\/\(.*\.INI\)/\1/g')

	echo "processing $ini_file_name..."
    #if grep -q gitMoveTestMode $PATH_TO_OXOG_JAR/$ini_file_name ; then
    #       sed -i.bak s/gitMoveTestMode=.*/$gitMoveTestMode/g $PATH_TO_OXOG_JAR/$ini_file_name
    #else
    #       echo "gitMoveTestMode=$gitMoveTestMode" >> $PATH_TO_OXOG_JAR/$ini_file_name
    #fi
	mv $ini_file_name $DEST_DIR/$ini_file_name
	update_or_append $DEST_DIR/$ini_file_name gitMoveTestMode $gitMoveTestMode
	update_or_append $DEST_DIR/$ini_file_name storageSource $storageSource
	update_or_append $DEST_DIR/$ini_file_name skipDownload $skipDownload
	update_or_append $DEST_DIR/$ini_file_name skipUpload $skipUpload
	#update_or_append $DEST_DIR/$ini_file_name refFile $refFile
	update_or_append $DEST_DIR/$ini_file_name uploadKey $uploadKey
	update_or_append $DEST_DIR/$ini_file_name gnosKey $gnosKey
	update_or_append $DEST_DIR/$ini_file_name skipOxoG $skipOxoG
	update_or_append $DEST_DIR/$ini_file_name skipVariantBam $skipVariantBam
	update_or_append $DEST_DIR/$ini_file_name skipAnnotation $skipAnnotation
	update_or_append $DEST_DIR/$ini_file_name uploadURL $uploadURL
	update_or_append $DEST_DIR/$ini_file_name JSONrepo $JSONrepo
	update_or_append $DEST_DIR/$ini_file_name JSONrepoName $JSONrepoName
	update_or_append $DEST_DIR/$ini_file_name JSONfolderName $JSONfolderName
	update_or_append $DEST_DIR/$ini_file_name JSONlocation $JSONlocation
	update_or_append $DEST_DIR/$ini_file_name GITemail $GITemail
	update_or_append $DEST_DIR/$ini_file_name GITname $GITname
	update_or_append $DEST_DIR/$ini_file_name GITPemFile $GITPemFile
	update_or_append $DEST_DIR/$ini_file_name studyRefNameOverride $studyRefNameOverride
	update_or_append $DEST_DIR/$ini_file_name svPadding $svPadding
	update_or_append $DEST_DIR/$ini_file_name snvPadding $snvPadding
	update_or_append $DEST_DIR/$ini_file_name indelPadding $indelPadding
	#update_or_append $DEST_DIR/$ini_file_name downloadMethod $downloadMethod
        update_or_append $DEST_DIR/$ini_file_name gnosMetadataUploadURL $gnosMetadataUploadURL
        update_or_append $DEST_DIR/$ini_file_name allowMissingFiles $allowMissingFiles
        update_or_append $DEST_DIR/$ini_file_name vcfDownloadMethod $vcfDownloadMethod
        update_or_append $DEST_DIR/$ini_file_name bamDownloadMethod $bamDownloadMethod
        update_or_append $DEST_DIR/$ini_file_name fileSystemSourcePath $fileSystemSourcePath
        update_or_append $DEST_DIR/$ini_file_name smufinDownloadMethod $smufinDownloadMethod
        update_or_append $DEST_DIR/$ini_file_name dummy $dummy
        update_or_append $DEST_DIR/$ini_file_name skipBamUpload $skipBamUpload
        update_or_append $DEST_DIR/$ini_file_name skipVcfUpload $skipVcfUpload

        # Now we inject smufin file names
        for tumour in $(cat $DEST_DIR/$ini_file_name  | grep -w tumour_aliquot_id_.\ =\ .* | sed 's/.*_\([0-9]\) = \(.*\)/\1:\2/g') ; do

                TUMOUR_ALIQUOT_ID=$(echo $tumour | sed -e 's/.*:\(.*\)/\1/g')
                TUMOUR_INDEX=$(echo $tumour | sed -e 's/\(.*\):.*/\1/g')
                if ! grep -q $TUMOUR_ALIQUOT_ID ~/donors_without_smufins.txt ; then
                        echo "adding tumour $tumour for smufin"
                        echo "smufin_indel_data_file_name_$TUMOUR_INDEX = $TUMOUR_ALIQUOT_ID.smufin.somatic.indel.vcf.gz"  >> $DEST_DIR/$ini_file_name
                        echo "smufin_indel_data_object_id_$TUMOUR_INDEX = $TUMOUR_ALIQUOT_ID.smufin.somatic.indel.vcf.gz"  >> $DEST_DIR/$ini_file_name
                        echo "smufin_indel_index_file_name_$TUMOUR_INDEX = $TUMOUR_ALIQUOT_ID.smufin.somatic.indel.vcf.gz.tbi"  >> $DEST_DIR/$ini_file_name
                        echo "smufin_indel_index_object_id_$TUMOUR_INDEX = $TUMOUR_ALIQUOT_ID.smufin.somatic.indel.vcf.gz.tbi"  >> $DEST_DIR/$ini_file_name
                else
                        echo "there is no smufin file for tumour $tumour so smufin will not be included in the new minibam"
                fi
        done;


done;
