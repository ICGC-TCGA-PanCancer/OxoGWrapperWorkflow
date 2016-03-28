package com.github.seqware.downloaders;

public class S3Downloader implements WorkflowFileDownloader {

	@Override
	public String getDownloadCommandString(String downloadDir, String workflowName, String... objectIDs) {
		
		if (workflowName == null || workflowName.trim().length() == 0)
		{
			throw new RuntimeException("workflowName cannot be null/empty!");
		}
		if (downloadDir == null || downloadDir.trim().length() == 0)
		{
			throw new RuntimeException("downloadDir cannot be null/empty!");
		}
		if (objectIDs.length == 0 || objectIDs[0] == null || objectIDs[0].trim().length() == 0)
		{
			throw new RuntimeException("objectIDs is null/empty or the first element is null/empty!");
		}

		
		//TODO: allow URL prefix to be parameterized from the INI file
		String getFilesCommand = "";
		String urlPrefix =  "s3://oicr.icgc/data/";
		for (String s : objectIDs)
		{
			String[] parts = s.split(":");
			String objectID = parts[0];
			String fileName = parts[1];
			getFilesCommand += " aws s3 cp "+urlPrefix+objectID + " " + downloadDir+"/"+ fileName + " ; \n";
		}
		getFilesCommand = " ( " + getFilesCommand + " ) ";
		return getFilesCommand;
	}

}
