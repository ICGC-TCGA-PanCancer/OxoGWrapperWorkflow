package com.github.seqware.downloaders;

/**
 * A downloader that downloads usign the ICGC Storage Client docker container.
 * @author sshorser
 *
 */
public class ICGCStorageDownloader implements WorkflowFileDownloader {

	public void setStorageSource(String storageSource)
	{
		this.storageSource = storageSource;
	}
	
	private String storageSource = null;
	
	/**
	 * @param downloadDir - the directory to download into.
	 * @param workflowName - used in the running docker container name.
	 * @param objectIDs - a list of all object IDs to download. a series of icgc-storage-client commands will be executed, one for each object ID.
	 * These commands will all be executed in the SAME container. 
	 */
	@Override
	public String getDownloadCommandString(String downloadDir, String workflowName, String ... objectIDs) {

		WorkflowFileDownloader.checkArgs(downloadDir, workflowName);
		if (this.storageSource == null || this.storageSource.trim().length() == 0)
		{
			throw new RuntimeException("storageSource cannot be null/empty!");
		}

		if (objectIDs.length == 0 || objectIDs[0] == null || objectIDs[0].trim().length() == 0)
		{
			throw new RuntimeException("objectIDs is null/empty or the first element is null/empty!");
		}
		
		
		String downloadObjects = "";
		for (String objectID : objectIDs)
		{
			downloadObjects += " /icgc/icgc-storage-client/bin/icgc-storage-client url --object-id "+objectID+" ;\n" 
				+ " /icgc/icgc-storage-client/bin/icgc-storage-client download --object-id " + objectID+" --output-layout bundle --output-dir /downloads/ ;\n "; 
		}
		
		String getFilesCommand = "(( docker run --rm --name get_"+workflowName+" "
				+ " -e STORAGE_PROFILE="+this.storageSource+" " 
			    + " -v "+downloadDir+"/logs/:/icgc/icgc-storage-client/logs/:rw "
				+ " -v /datastore/credentials/collab.token:/icgc/icgc-storage-client/conf/application.properties:ro "
			    + " -v "+downloadDir+"/:/downloads/:rw"
	    		+ " icgc/icgc-storage-client:1.0.13 /bin/bash -c \" "+downloadObjects+" \" ) && sudo chmod a+rw -R /datastore/vcf )";

		return getFilesCommand;
	}

}
