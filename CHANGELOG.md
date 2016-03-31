# CHANGELOG

## 1.1.4
 - Hotfix: Fixed a bug where the Sanger SNV index file was being referenced instead of the SV file. This was causing problems for Workers that used the S3 download method as two index files would get downloaded with the same name, and then the job that stats all files to ensure that they all exist would fail.
 

## 1.1.3
 - stat all files that have been downloaded - sometimes gtdownload will not download a file but the exit code is still 0 so we must `stat` the files to make sure they were actually downloaded properly.
 - gtdownload process now takes two keys: one for BAMs and once for VCFs. This is for situations where BAMs and VCFs are not hosted in the same place and require different keys for download.
 - fixes for git_mv script: updated file should now be committed instead of overwritten.

## 1.1.2
 - Fixes to git_mv script

## 1.1.1
 - Use version 1.0.13 of icgc-storage-client
 - Fix for issue with AWS credentials being clobbered by the launcher
 - Fix git_mv to set timestamps inside loop

## 1.1.0
 - Input files can now be downloaded using gtdownload or AWS CLI. To do this, add a new property to you INI file: downloadMethod.
This can have one of three options:
	- gtdownload
	- icgcStorageClient
	- s3 (You will need to put your AWS credentials in ~/.gnos on the launcher for this to work).
 - The timestamps of workflow state transitions will be injected in the JSON files before they are moved in github. They will look something like this: "transition_to_downloading-jobs_timestamp":"2016-03-18T15:00:00"
 - Workflow now uses icgc-storage-client version 1.0.12

## 1.0.4
 - Fixed a bug in git_move.py

## 1.0.3
 - Changed pre-processing to replace leading M with MT in the CHROM field. This was causing bcf-tools norm to fail.
	At least one Broad INDEL had M (which is not valid) instead of MT, so it will be fixed at workflow run-time. 
 
## 1.0.2
 - Fixed the git_move.py script to move files in git better.

## 1.0.1
**Main changes:**
 - Add `git reset --hard origin/master` to git move scripts.
 - Add `-t` and `-u` to rsync command, trying to resolve the manifest-newer-than-igto issue that Jonthan reported.

**Other changes:**
 - update ini_generator.sh script with more current default values.
 - Code cleanup in INI Generator.
 - Update Dockerfile (even though it's not used right now).
 - Update SeqWare artifact dependency to seqware-bin-linux-x86-64-jre-8.0.45
 
## 1.0.0
Initial release.