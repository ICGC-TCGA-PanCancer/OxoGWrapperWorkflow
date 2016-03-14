package com.github.seqware;

/**
 * Creates an instance of WorkflowFileDownloader.
 * @author sshorser
 *
 */
public abstract class DownloaderFactory {

	enum DownloadMethod
	{
		gtdownload, icgc_storage_client, s3;
	}
	
	static public WorkflowFileDownloader createDownloader(DownloadMethod downloadMethod)
	{
		WorkflowFileDownloader downloader = null;
		
		switch (downloadMethod) {
		case icgc_storage_client:
			downloader = new ICGCStorageDownloader();
			break;
		case gtdownload:
			downloader = new GNOSDownloader();
			break;
		//TODO: implement a downloader for S3
		default:
			throw new RuntimeException("download method: "+downloadMethod+" is unknown! Aborting.");
		}
		

		return downloader;
	}
	
}
