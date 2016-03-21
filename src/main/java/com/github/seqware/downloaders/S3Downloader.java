package com.github.seqware.downloaders;

public class S3Downloader implements WorkflowFileDownloader {

	private String fileName;

	public S3Downloader(String fileName) {
		this.fileName = fileName;
	}
	
	@Override
	public String getDownloadCommandString(String downloadDir, String workflowName, String... objectIDs) {
		
		String getFilesCommand = "( aws s3 cp s3://oicr.icgc/data/"+objectIDs[0]+"  "+fileName+")";
		
		return getFilesCommand;
	}

}
