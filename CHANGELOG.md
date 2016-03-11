# CHANGELOG

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