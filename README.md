# OxoG Wrapper Workflow

This workflow is a wrapper around the OxoG component. It will download VCFs (from all three workflows: Sanger, DKFZ/EMBL, Broad) and BAM files (tumour and normal), run OxoG on them, and then upload the results via rsync.

The VCF input files will be processed using `bcftools norm` on the indel VCF, and then `vcfcombine` will be used to combine them all into a single VCF.

## Building

This will build both the Docker image *and* the workflow inside.

    docker build -t pancancer/pcawg-oxog-merge-workflow:1.0 .

## Building Just the Workflow

This will just build the Java portion of the workflow and not the Docker image.

    mvn clean install

## Usage

1. Ensure that you have a valid Collaboratory token in `~/.gnos`.
2. Ensure that you have a valid git pem file in `~/.gnos`.
3. Call the workflow like this:

```
sudo docker run --rm -v /datastore:/datastore \
					-v /workflows/Workflow_Bundle_OxoGWrapper_1.0_SeqWare_1.1.2/:/workflow/ \
					-v /var/run/docker.sock:/var/run/docker.sock \
					-v /home/ubuntu/MyINIFile.INI:/ini \
					-v /home/ubuntu/.gnos/:/home/seqware/.gnos \
		pancancer/seqware_whitestar_pancancer:1.1.2 \
	seqware bundle launch --dir /workflow --ini /ini --no-metadata --engine whitestar-parallel
```

### INIs
The INI must reference the object IDs and filenames of INDEL, SNV, and SV VCFs from the Sanger, Broad, Muse (SNV only), and DKFZ/EMBL pipelines.
Populating such an INI file is a lot of work and since most of the necessary information is already in the JSON file (produced by Linda/Junjun),
you can use the INIGenerator to produce an INI for you. It works like this:

```
cd /workflows/Workflow_Bundle_OxoGWrapper_1.0_SeqWare_1.1.2/Workflow_Bundle_OxoGWrapper/1.0/
java -cp ./classes:./lib com.github.seqware.INIGenerator ~/BTCA-SG.BTCA_donor_A153.json
```

Other fields that are useful to populate in the INI:

 - gitMoveTestMode - Set it to `true` to prevent file operations from being propagated back to github. This is really only intended for testing purposes.
 - storageSource - This tells the ICGC storage client where to download from. Options are `aws` or `collab`.
 - skipDownload - Set to `true` to skip file downloading. Useful if you can set up the environment with the files ahead of time.
 - skipUpload - Set to `true` to skip the upload process.
 - refFile - Specify the reference fasta file to use in the workflow. Must be in a directory _relative_ to `/datastore/refdata/`. Defaults to `public/Homo_sapiens_assembly19.fasta`.
 - uploadKey - The path to the key that will be used when uploading to the rsync server.
 - gnosKey - The path to the key that will be used when talking to GNOS to generate gto files and metadata.
 - skipOxoG - Skip the OxoG filtering process. Useful if OxoG has already run on this data.
 - skipVariantBam - Skip running variant to produce the minibams. Useful if you do not need to run this step.
 - skipAnnotation - Skip running the annotation process. Useful if you have already run this step.


### Flow Control

It uses git to note state in the workflow... more details TBD.

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
