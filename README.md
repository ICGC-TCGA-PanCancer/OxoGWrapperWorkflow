# OxoG Wrapper Workflow

This workflow is a wrapper around the OxoG component. It will download VCFs (from all three workflows: Sanger, DKFZ/EMBL, Broad) and BAM files (tumour and normal), run OxoG on them, and then upload the results via rsync.

The VCF input files will be processed using `bcftools norm` on the indel VCF, and then `vcfcombine` will be used to combine them all into a single VCF.

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