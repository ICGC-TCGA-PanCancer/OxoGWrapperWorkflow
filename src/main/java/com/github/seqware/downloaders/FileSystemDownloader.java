package com.github.seqware.downloaders;

import java.util.Arrays;

// NOTE: smuffin will come from NFS always but the BAMs will come from remote source.
// Also, smuffin pipeline only will supply INDELs and only needs to have minibam generation run.

/**
 * This downloader will copy the input files from a path in the filesystem. This is to be used 
 * when the input files are on a filesystem such as NFS.
 * @author sshorser
 *
 */
public class FileSystemDownloader implements WorkflowFileDownloader {

	private String sourcePathDirectory;
	
	
	
	/**
	 * Returns a command that can be used to retrieve files.
	 * @param destinationDir - The directory in which to place the files.
	 * @param workflowName - The workflow name. Not used for this downloader.
	 * @param fileNames - A list of file names to copy.
	 */
	@Override
	public String getDownloadCommandString(String destinationDir, String workflowName, String... fileNames) {
		WorkflowFileDownloader.checkArgs(destinationDir, "DUMMY_VALUE");
		
		if (this.sourcePathDirectory == null || this.sourcePathDirectory.trim().equals(""))
		{
			throw new RuntimeException("You must give a valid source directory!");
		}
		if (fileNames.length == 0)
		{
			throw new RuntimeException("You must give a list of files!");
		}
		if (Arrays.stream(fileNames).allMatch(p -> p == null || p.trim().length()==0))
		{
			throw new RuntimeException("NONE of the filenames you gave were valid, they were NULL or emptry strings!");
		}
		
		String getFilesCommand = "(sudo mkdir -p "+destinationDir+"/ && sudo chmod a+rw "+destinationDir+" \n";
		for(String fileName : fileNames)
		{
			if (fileName.trim().length() != 0)
			{
				getFilesCommand += " cp "+sourcePathDirectory+"/"+fileName+" "+destinationDir+"/"+fileName+"\n";
			}
			else
			{
				System.out.println("A fileName for "+workflowName+" that is an empty string was found.");
			}
		}
		getFilesCommand += " ) ";
		return getFilesCommand;
	}



	public void setSourcePathDirectory(String sourcePathDirectory) {
		this.sourcePathDirectory = sourcePathDirectory;
	}

}
