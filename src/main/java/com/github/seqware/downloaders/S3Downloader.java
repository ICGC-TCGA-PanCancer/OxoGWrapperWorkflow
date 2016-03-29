package com.github.seqware.downloaders;

public class S3Downloader implements WorkflowFileDownloader {

	@Override
	public String getDownloadCommandString(String downloadDir, String workflowName, String... objectIDs) {
		WorkflowFileDownloader.checkArgs(downloadDir, workflowName);

		if (objectIDs.length == 0 || objectIDs == null)
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
