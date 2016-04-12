package com.github.seqware;

import com.github.seqware.OxoGWrapperWorkflow.Pipeline;
import com.github.seqware.OxoGWrapperWorkflow.VCFType;

public class VcfInfo {

	private String fileName;
	private String indexFileName;
	private String objectID;
	private String indexObjectID;
	private VCFType vcfType;
	private Pipeline originatingPipeline;
	private String originatingTumourAliquotID;
	private long fileSize;
	private String pipelineGnosID;
	
	public String getFileName() {
		return this.fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public String getIndexFileName() {
		return this.indexFileName;
	}
	public void setIndexFileName(String indexFileName) {
		this.indexFileName = indexFileName;
	}
	public String getObjectID() {
		return this.objectID;
	}
	public void setObjectID(String objectID) {
		this.objectID = objectID;
	}
	public VCFType getVcfType() {
		return this.vcfType;
	}
	public void setVcfType(VCFType vcfType) {
		this.vcfType = vcfType;
	}
	public Pipeline getOriginatingPipeline() {
		return this.originatingPipeline;
	}
	public void setOriginatingPipeline(Pipeline originatingPipeline) {
		this.originatingPipeline = originatingPipeline;
	}
	public String getOriginatingTumourAliquotID() {
		return originatingTumourAliquotID;
	}
	public void setOriginatingTumourAliquotID(String originatingTumourAliquotID) {
		this.originatingTumourAliquotID = originatingTumourAliquotID;
	}
	public long getFileSize() {
		return fileSize;
	}
	public void setFileSize(long fileSize) {
		this.fileSize = fileSize;
	}
	public String getIndexObjectID() {
		return indexObjectID;
	}
	public void setIndexObjectID(String indexObjectID) {
		this.indexObjectID = indexObjectID;
	}
	public String getPipelineGnosID() {
		return pipelineGnosID;
	}
	public void setPipelineGnosID(String pipelineGnosID) {
		this.pipelineGnosID = pipelineGnosID;
	}
}
