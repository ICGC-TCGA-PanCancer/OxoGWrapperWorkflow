package com.github.seqware.jobgenerators;

import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class JobGeneratorBase {

	protected Collector<String[], ?, Map<String, Object>> collectToMap = Collectors.toMap(kv -> kv[0], kv -> kv[1]);
	protected String JSONlocation;
	protected String JSONrepoName;
	protected String JSONfolderName;
	protected String JSONfileName;
	protected boolean gitMoveTestMode;

	public JobGeneratorBase(String location, String repoName, String folderName, String fileName)
	{
		this.setJSONFileInfo(location, repoName, folderName, fileName);
	}
	
	public void setJSONFileInfo(String location, String repoName, String folderName, String fileName)
	{
		this.setJSONlocation(location);
		this.setJSONrepoName(repoName);
		this.setJSONfolderName(folderName);
		this.setJSONfileName(fileName);
	}
	
	public JobGeneratorBase() {
		super();
	}

	public String getJSONlocation() {
		return this.JSONlocation;
	}

	public void setJSONlocation(String jSONlocation) {
		this.JSONlocation = jSONlocation;
	}

	public String getJSONrepoName() {
		return this.JSONrepoName;
	}

	public void setJSONrepoName(String jSONrepoName) {
		this.JSONrepoName = jSONrepoName;
	}

	public String getJSONfolderName() {
		return this.JSONfolderName;
	}

	public void setJSONfolderName(String jSONfolderName) {
		this.JSONfolderName = jSONfolderName;
	}

	public String getJSONfileName() {
		return this.JSONfileName;
	}

	public void setJSONfileName(String jSONfileName) {
		this.JSONfileName = jSONfileName;
	}

	public boolean isGitMoveTestMode() {
		return this.gitMoveTestMode;
	}

	public void setGitMoveTestMode(boolean gitMoveTestMode) {
		this.gitMoveTestMode = gitMoveTestMode;
	}

}