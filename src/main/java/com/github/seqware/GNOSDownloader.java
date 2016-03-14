package com.github.seqware;

public class GNOSDownloader implements WorkflowFileDownloader {

	/**
	 * Download from GNOS.
	 * @param downloadDir - the directory to download into.
	 * @param workflowName - the name of the workflow, this will be appeneded to downloadDir, and will also be usd in the container name for the docker container that runs the download.
	 * @param objectIDs - if you are using gtdownload, only the first item in this list will be used, since it will download by GNOS ID, and the GNOS ID will refer to a SET of files. 
	 */
	@Override
	public String getDownloadCommandString(String downloadDir, String workflowName, String ... objectIDs) {

		String getFilesCommand = "( docker run --rm --name get_vcf_"+workflowName+" "
				+ " -v /datastore/credentials/gnos.key:/gnos.key "
			    + " -v "+downloadDir+"/:/downloads/:rw"
	    		+ " pancancer/pancancer_upload_download:1.7 /bin/bash -c \""
	    			+ "gtdownload -k 30 --peer-timeout 120 -p /downloads/ -l /downloads/gtdownload.log -c /gnos.key "+objectIDs[0]+" \" ) ";

		return getFilesCommand;
	}


}
