package com.github.seqware.downloaders;

/**
 * Creates an instance of WorkflowFileDownloader.
 * @author sshorser
 *
 */
public abstract class DownloaderFactory {

	public enum DownloadMethod
	{
		gtdownload, icgcStorageClient, s3
	}
	
	static public WorkflowFileDownloader createDownloader(DownloadMethod downloadMethod, String ... args)
	{
		WorkflowFileDownloader downloader = null;
		
		switch (downloadMethod) {
		case icgcStorageClient:
			String storageSource = args[0];
			downloader = new ICGCStorageDownloader(storageSource);
			break;
		case gtdownload:
			downloader = new GNOSDownloader();
			break;
		case s3:
			downloader = new S3Downloader();
			break;
		default:
			throw new RuntimeException("download method: "+downloadMethod+" is unknown! Aborting.");
		}
		

		return downloader;
	}
	
}
