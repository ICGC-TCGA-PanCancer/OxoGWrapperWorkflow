package com.github.seqware.downloaders;

/**
 * Creates an instance of WorkflowFileDownloader.
 * @author sshorser
 *
 */
public class DownloaderFactory {

	public enum DownloadMethod
	{
		gtdownload, icgcStorageClient, s3
	}

	static public WorkflowFileDownloader createDownloader(DownloadMethod downloadMethod, String ... args)
	{
		WorkflowFileDownloader downloader;
		
		switch (downloadMethod)
		{
			case icgcStorageClient:
				return DownloaderBuilder.of(ICGCStorageDownloader::new).with(ICGCStorageDownloader::setStorageSource, args[0]).build();
			case gtdownload:
				return downloader = DownloaderBuilder.of(GNOSDownloader::new).with(GNOSDownloader::setDownloadKey, args[0]).build();
			case s3:
				return downloader = DownloaderBuilder.of(S3Downloader::new).build();
			default:
				throw new RuntimeException("Unknown downloadMethod: "+downloadMethod.toString());
		}
	}
	
}
