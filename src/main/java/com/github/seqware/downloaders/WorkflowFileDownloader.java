package com.github.seqware.downloaders;

/**
 * Interface for objects that will generate command strings that are used for different types of downloads.
 * 
 * These objects won't <i>actually</i> perform a download, they will return a string that can be used to execute a
 * specific download command.
 * @author sshorser
 *
 */
public interface WorkflowFileDownloader {
	
	/**
	 * Get the string that can be used to execute a download.
	 * @param downloadDir - The directory that the output should be in.
	 * @param workflowName - The workflowName of the downloaded objects.
	 * @param objectIDs - A list of the object IDs.
	 * @return
	 */
	String getDownloadCommandString(String downloadDir, String workflowName, String ... objectIDs);
	
	static void checkArgs(String downloadDir, String workflowName)
	{
		if (workflowName == null || workflowName.trim().length() == 0)
		{
			throw new RuntimeException("workflowName cannot be null/empty!");
		}
		if (downloadDir == null || downloadDir.trim().length() == 0)
		{
			throw new RuntimeException("downloadDir cannot be null/empty!");
		}
	}
}
