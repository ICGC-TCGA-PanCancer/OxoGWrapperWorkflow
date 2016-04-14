package com.github.seqware;

import com.github.seqware.OxoGWrapperWorkflow.DownloadMethod;
import com.github.seqware.downloaders.DownloaderBuilder;
import com.github.seqware.downloaders.GNOSDownloader;
import com.github.seqware.downloaders.ICGCStorageDownloader;
import com.github.seqware.downloaders.S3Downloader;

public class DownloadUtils {

	/**
	 * Generates a string to be used as a download command for a file or files.
	 * @param downloadMethod - The download method.
	 * @param outDir - The directory to download into.
	 * @param downloadType - The type of download, such as "Normal" or "Sanger"
	 * @param storageSource - The storage source (needed when downloadMethod == icgcStorageClient)
	 * @param downloadKey - The download key (needed when downloadMethod == gtdownload)
	 * @param objectIDs - The list of objects to download.
	 * @return
	 */
	//TODO: Does this really need to be in a separate Utility class? Maybe move it back in to the main workflow class.
	static String getFileCommandString(DownloadMethod downloadMethod, String outDir, String downloadType, String storageSource, String downloadKey, String ... objectIDs  )
	{
		switch (downloadMethod)
		{
			case icgcStorageClient:
				//System.out.println("DEBUG: storageSource: "+this.storageSource);
				return ( DownloaderBuilder.of(ICGCStorageDownloader::new).with(ICGCStorageDownloader::setStorageSource, storageSource).build() ).getDownloadCommandString(outDir, downloadType, objectIDs);
			case gtdownload:
				//System.out.println("DEBUG: gtDownloadKey: "+this.gtDownloadVcfKey);
				return ( DownloaderBuilder.of(GNOSDownloader::new).with(GNOSDownloader::setDownloadKey, downloadKey).build() ).getDownloadCommandString(outDir, downloadType, objectIDs);
			case s3:
				return ( DownloaderBuilder.of(S3Downloader::new).build() ).getDownloadCommandString(outDir, downloadType, objectIDs);
			default:
				throw new RuntimeException("Unknown downloadMethod: "+downloadMethod.toString());
		}
	}
}
