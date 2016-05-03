package com.github.seqware;

public class TumourInfo {
	private String tumourMetdataURL;
	private String tumourBAMFileName;
	private String tumourBamGnosID;
	private String tumourMinibamPath;
	private String tumourBamGNOSRepoURL;
	private String tumourBamIndexFileName;
	private String bamTumourIndexObjectID;
	private String bamTumourObjectID;
	private String aliquotID;

	public String getTumourMetdataURL() {
		return this.tumourMetdataURL;
	}

	public void setTumourMetdataURL(String tumourMetdataURL) {
		this.tumourMetdataURL = tumourMetdataURL;
	}

	public String getTumourBAMFileName() {
		return this.tumourBAMFileName;
	}

	public void setTumourBAMFileName(String tumourBAMFileName) {
		this.tumourBAMFileName = tumourBAMFileName;
	}

	public String getTumourBamGnosID() {
		return this.tumourBamGnosID;
	}

	public void setTumourBamGnosID(String tumourBamGnosID) {
		this.tumourBamGnosID = tumourBamGnosID;
	}

	public String getTumourMinibamPath() {
		return this.tumourMinibamPath;
	}

	public void setTumourMinibamPath(String tumourMinibamPath) {
		this.tumourMinibamPath = tumourMinibamPath;
	}

	public String getTumourBamGNOSRepoURL() {
		return this.tumourBamGNOSRepoURL;
	}

	public void setTumourBamGNOSRepoURL(String tumourBamGNOSRepoURL) {
		this.tumourBamGNOSRepoURL = tumourBamGNOSRepoURL;
	}

	public String getTumourBamIndexFileName() {
		return this.tumourBamIndexFileName;
	}

	public void setTumourBamIndexFileName(String tumourBamIndexFileName) {
		this.tumourBamIndexFileName = tumourBamIndexFileName;
	}

	public String getBamTumourObjectID() {
		return this.bamTumourObjectID;
	}

	public void setBamTumourObjectID(String bamTumourObjectID) {
		this.bamTumourObjectID = bamTumourObjectID;
	}

	public String getBamTumourIndexObjectID() {
		return this.bamTumourIndexObjectID;
	}

	public void setBamTumourIndexObjectID(String bamTumourIndexObjectID) {
		this.bamTumourIndexObjectID = bamTumourIndexObjectID;
	}

	public String getAliquotID() {
		return aliquotID;
	}

	public void setAliquotID(String aliquotID) {
		this.aliquotID = aliquotID;
	}
}