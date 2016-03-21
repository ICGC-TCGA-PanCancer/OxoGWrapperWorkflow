package com.github.seqware;

/**
 * A downloader that downloads usign the ICGC Storage Client docker container.
 * @author sshorser
 *
 */
public class ICGCStorageDownloader implements WorkflowFileDownloader {

	public ICGCStorageDownloader(String storageSource)
	{
		this.storageSource = storageSource;
	}
	
	private String storageSource;
	
	/**
	 * @param downloadDir - the directory to download into.
	 * @param workflowName - used in the running docker container name.
	 * @param objectIDs - a list of all object IDs to download. a series of icgc-storage-client commands will be executed, one for each object ID.
	 * These commands will all be executed in the SAME container. 
	 */
	@Override
	public String getDownloadCommandString(String downloadDir, String workflowName, String ... objectIDs) {

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
	    		+ " icgc/icgc-storage-client:1.0.12 /bin/bash -c \" "+downloadObjects+" \" ) && sudo chmod a+rw -R /datastore/vcf )";

		return getFilesCommand;
	}

}
