# OxoG Wrapper Workflow

## Overview
This workflow will download all VCF files for all analysis pipelines (Broad, DKFZ/EMBL, Muse, Sanger) and all BAM files (normal and all tumour BAMs) to
perform the various analysis tasks.

This workflow will perform three tasks:
 - OxoG filtering
 - Mini-bam generation
 - Annotation

### OxoG Filtering
This is a component by the Broad institute that will perform filtering on VCF files.

### Mini-bam generation
Mini-bam files are produced by a program called variantbam.

### Annotation
This is Jonathan Dursi's Annotator. See:
 - https://hub.docker.com/r/ljdursi/pcawg-annotate/ 
 - https://github.com/ljdursi/sga-annotate-docker

## Building

This will just compile and package the Java portion of the workflow:

    mvn clean package

To produce a SeqWare bundle:

	seqware bundle package --dir target/Workflow_Bundle_OxoGWrapper_*_SeqWare_1.1.2

### Usage

1. Ensure that you have a valid Collaboratory token in `~/.gnos`.
2. Ensure that you have a valid git pem file in `~/.gnos`.
3. Call the workflow like this:

```
	docker run --rm -v /datastore/:/datastore/ \
		-v /workflows/Workflow_Bundle_OxoGWrapper_2.0.0-beta_SeqWare_1.1.2/:/workflow/ \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v /home/ubuntu/.gnos/:/home/ubuntu/.gnos/ \
		-v /home/ubuntu/SomeIniFile.INI:/ini \
		pancancer/seqware_whitestar_pancancer:1.1.2-actual-java8 \
			/bin/bash -c "seqware bundle launch --dir /workflow --ini /ini --no-metadata --engine whitestar-parallel"
```

## Building as a docker image

This will build a new image which contains the workflow as a SeqWare bundle:

    docker build -t pancancer/pcawg-oxog-wrapper-workflow:x.x.x .

### Usage

1. Ensure that you have a valid Collaboratory token in `~/.gnos`.
2. Ensure that you have a valid git pem file in `~/.gnos`.
3. Call the workflow like this (note: you do not have to mount the workflows directory in this case, because the workflow is already inside the container):

```
sudo docker run --rm -v /datastore:/datastore \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v /home/ubuntu/MyINIFile.INI:/ini \
		-v /home/ubuntu/.gnos/:/home/seqware/.gnos \
	pancancer/pcawg-oxog-wrapper-workflow:x.x.x \
		seqware bundle launch --dir /workflow --ini /ini --no-metadata --engine whitestar-parallel
```

### INIs
The INI must reference the object IDs and filenames of INDEL, SNV, and SV VCFs from the Sanger, Broad, Muse (SNV only), and DKFZ/EMBL pipelines.
Populating such an INI file is a lot of work and since most of the necessary information is already in the JSON file (produced by Linda/Junjun),
you can use the INIGenerator to produce an INI for you. It works like this:

```
cd /workflows/Workflow_Bundle_OxoGWrapper_1.0_SeqWare_1.1.2/Workflow_Bundle_OxoGWrapper/1.0/
java -cp ./classes:./bin com.github.seqware.INIGenerator ~/BTCA-SG.BTCA_donor_A153.json
```

Other fields that are useful to populate in the INI:

 - gitMoveTestMode - Set it to `true` to prevent file operations from being propagated back to github. This is really only intended for testing purposes.
 - storageSource - This tells the ICGC storage client where to download from. Options are `aws` or `collab`.
 - skipDownload - Set to `true` to skip file downloading. Useful if you can set up the environment with the files ahead of time.
 - skipUpload - Set to `true` to skip the upload process.
 - skipOxoG - Skip the OxoG filtering process. Useful if OxoG has already run on this data.
 - skipVariantBam - Skip running variant to produce the minibams. Useful if you do not need to run this step.
 - skipAnnotation - Skip running the annotation process. Useful if you have already run this step.
 - refFile - Specify the reference fasta file to use in the workflow. Must be in a directory _relative_ to `/datastore/refdata/`. Defaults to `public/Homo_sapiens_assembly19.fasta`.
 - uploadKey - The path to the key that will be used when uploading to the rsync server.
 - gnosKey - The path to the key that will be used when talking to GNOS to generate gto files and metadata.
 - rsyncKey - The path to the key that will be used when the results are rsynced.
 - uploadURL - The URL to use in the rsync command at the end of the workflow, such as: `someUser@10.10.10.10:~/incoming/oxog_results/`
 - downloadMethod - Which method to use to download files. Default will be to use the icgc storage client (`icgcStorageClient`), but you can also specify `gtdownload` or `s3` (which will use `aws s3`).
 - storageSource - Where to download files from, if the `downloadMethod` is `icgcStorageClient`. Defaults to `collab` but if you are using the icgc storage client in AWS, you will want to specify `aws`.


### Flow Control

It uses git to note state in the workflow.

To do this, a repo must be set up with a directory that contains the state-directories. When a JSON file is in a specific directory, in indicates the state that the workflow running that JSOn file is in.

The JSON file for a job must first be in the `queued-jobs` directory. The workflow will then move it to the `downloading-jobs` directory. When the download phase of the workflow completes, the file is moved to the `running-jobs` directory.
When the main processes have finished and the upload is in progress, the file will be moved to the `uploading-jobs` directory. When upload completes successfully, the file will be moved to the `completed-jobs` directory.
If the workflow fails, the file may be moved to the `failed-jobs` directory. If you re-run a workflow on a worker manually, be aware that you'll have to move the JSON file back to the directory that the workflow expects it to be in.
  

### Downloads

The inputs are 1) all the variant calling workflow outputs (Sanger, DKFZ/EMBL, EMBL, Muse) and
2) the BAM files for normal and tumour(s). 

These are downloaded from either AWS S3 or the Collaboratory using the ICGC Storage Client.

See TODO for more information.

### Uploads

Uploads are 1) the merged/normalized/OxoG filtered variant calls, specifics depend on the
variant type. 2) the mini-bams for normal and tumour(s).

## TODO

* need support for inputs from (tentatively) 1) local file paths and 2) GNOS directly (specifically CGHub for TCGA donors that aren't on S3 or Collaboratory)
