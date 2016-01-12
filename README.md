# OxoG Wrapper Workflow

This workflow is a wrapper around the OxoG component. It will download VCF and BAM files, run OxoG on them, and then upload the results via rsync.

The VCF input files will be processed using vcfallelicprimitives on the indel, and then vcfcombine will be used to combine them all into a single VCF.