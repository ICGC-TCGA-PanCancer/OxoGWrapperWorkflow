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
