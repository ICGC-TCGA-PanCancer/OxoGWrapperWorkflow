#! /bin/bash

# User should configure these:

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

PATH_TO_JSON_DIR=$1
PATH_TO_OXOG_CLASSES=$2

function update_or_append
{
	file=$1
	search_string=$2
	replace_string=$3
	append_string="$search_string=$replace_string"
	
	if grep -q $search_string $file ; then
		sed -i.bak s/$search_string=.*/$search_string=$replace_string/g $file
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
	#	sed -i.bak s/gitMoveTestMode=.*/$gitMoveTestMode/g $PATH_TO_OXOG_JAR/$ini_file_name
	#else
	#	echo "gitMoveTestMode=$gitMoveTestMode" >> $PATH_TO_OXOG_JAR/$ini_file_name
	#fi
	
	update_or_append $PATH_TO_JSON_DIR/$ini_file_name gitMoveTestMode $gitMoveTestMode
	update_or_append $PATH_TO_JSON_DIR/$ini_file_name storageSource $storageSource
	update_or_append $PATH_TO_JSON_DIR/$ini_file_name skipDownload $skipDownload 
	update_or_append $PATH_TO_JSON_DIR/$ini_file_name skipUpload $skipUpload
	update_or_append $PATH_TO_JSON_DIR/$ini_file_name refFile $refFile
	update_or_append $PATH_TO_JSON_DIR/$ini_file_name uploadKey $uploadKey
	update_or_append $PATH_TO_JSON_DIR/$ini_file_name gnosKey $gnosKey
	update_or_append $PATH_TO_JSON_DIR/$ini_file_name skipOxoG $skipOxoG
	update_or_append $PATH_TO_JSON_DIR/$ini_file_name skipVariantBam $skipVariantBam 
	update_or_append $PATH_TO_JSON_DIR/$ini_file_name skipAnnotation $skipAnnotation
	update_or_append $PATH_TO_JSON_DIR/$ini_file_name uploadURL $uploadURL
	update_or_append $PATH_TO_JSON_DIR/$ini_file_name JSONrepo $JSONrepo
	update_or_append $PATH_TO_JSON_DIR/$ini_file_name JSONrepoName $JSONrepoName
	update_or_append $PATH_TO_JSON_DIR/$ini_file_name JSONfolderName $JSONfolderName
	update_or_append $PATH_TO_JSON_DIR/$ini_file_name JSONlocation $JSONlocation
	update_or_append $PATH_TO_JSON_DIR/$ini_file_name GITemail $GITemail
	update_or_append $PATH_TO_JSON_DIR/$ini_file_name GITname $GITname
	update_or_append $PATH_TO_JSON_DIR/$ini_file_name GITPemFile $GITPemFile
done;
