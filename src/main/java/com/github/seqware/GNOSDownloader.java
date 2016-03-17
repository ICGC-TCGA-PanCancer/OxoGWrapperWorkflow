package com.github.seqware;

public class GNOSDownloader implements WorkflowFileDownloader {

	/**
	 * Download from GNOS.
	 * @param downloadDir - the directory to download into.
	 * @param workflowName - the name of the workflow, this will be appeneded to downloadDir, and will also be usd in the container name for the docker container that runs the download.
	 * @param objectIDs - if you are using gtdownload, only the first item in this list will be used.
	 * 						This should be the *URL* to the object you want, such as: https://gtrepo-ebi.annailabs.com/cghub/data/analysis/download/96e252b8-911a-44c7-abc6-b924845e0be6 
	 */
	@Override
	public String getDownloadCommandString(String downloadDir, String workflowName, String ... objectIDs) {

		String getFilesCommand = "( docker run --rm --name get_vcf_"+workflowName+" "
				+ " -v /datastore/credentials/gnos.key:/gnos.key "
			    + " -v "+downloadDir+"/:/downloads/:rw"
	    		+ " pancancer/pancancer_upload_download:1.7 /bin/bash -c \""
	    			+ "sudo gtdownload -k 30 --peer-timeout 120 -p /downloads/ -l /downloads/gtdownload.log -c /gnos.key "+objectIDs[0]+" \" ) ";

		return getFilesCommand;
	}


}
