package com.github.seqware.jobgenerators;

import java.util.Arrays;

import com.github.seqware.GitUtils;
import com.github.seqware.OxoGWrapperWorkflow.BAMType;
import com.github.seqware.TemplateUtils;

import net.sourceforge.seqware.pipeline.workflowV2.AbstractWorkflowDataModel;
import net.sourceforge.seqware.pipeline.workflowV2.model.Job;

public class VariantBamJobGenerator extends JobGeneratorBase{

	@FunctionalInterface
	public interface UpdateBamForUpload<S,T>
	{
		public void accept(S s, T t);
	}
	
	public VariantBamJobGenerator(String JSONlocation, String JSONrepoName, String JSONfolderName, String JSONfileName) {
		super(JSONlocation, JSONrepoName, JSONfolderName, JSONfileName);
	}
	

	private String tumourAliquotID;
	private String snvPadding;
	private String svPadding;
	private String indelPadding;
	private String snvVcf;
	private String svVcf;
	private String indelVcf;
	
	public Job doVariantBam(AbstractWorkflowDataModel workflow,BAMType bamType, String bamName, String bamPath, String tumourBAMFileName, String tumourID, UpdateBamForUpload<String,String> updateFilesForUpload, Job ...parents)
	{
		Job runVariantbam = workflow.getWorkflow().createBashJob("run "+bamType+(bamType==BAMType.tumour?"_"+tumourID+"_":"")+" variantbam");

		String minibamName = "";
		if (bamType == BAMType.normal)
		{
			minibamName = bamName.replace(".bam", "_minibam");
			String normalMinibamPath = "/datastore/variantbam_results/"+minibamName+".bam";
			updateFilesForUpload.accept(normalMinibamPath,null);
			updateFilesForUpload.accept(normalMinibamPath+".bai",null);
		}
		else
		{
			minibamName = tumourBAMFileName.replace(".bam", "_minibam");
			String tumourMinibamPath = "/datastore/variantbam_results/"+minibamName+".bam";
			updateFilesForUpload.accept(tumourMinibamPath,tumourAliquotID);
			updateFilesForUpload.accept(tumourMinibamPath+".bai",tumourAliquotID);
		}
		
		String command = TemplateUtils.getRenderedTemplate(Arrays.stream( new String[][] {
			{ "containerNameSuffix", bamType + (bamType == BAMType.tumour ? "_with_tumour_"+tumourID:"") },
			{ "minibamName", minibamName+".bam"},  {"snvPadding", String.valueOf(this.snvPadding)}, {"svPadding", String.valueOf(this.svPadding)},
			{ "indelPadding", String.valueOf(this.indelPadding) }, { "pathToBam", bamPath },
			{ "snvVcf", snvVcf }, { "svVcf", svVcf }, { "indelVcf", indelVcf }
		}).collect(this.collectToMap), "runVariantbam.template" );
		
		String moveToFailed = GitUtils.gitMoveCommand("running-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, workflow.getWorkflowBaseDir() + "/scripts/");
		command += (" || " + moveToFailed);
		runVariantbam.setCommand(command);

		for (Job parent : parents)
		{
			runVariantbam.addParent(parent);
		}
		
		return runVariantbam;
	}

	public String getTumourAliquotID() {
		return this.tumourAliquotID;
	}

	public void setTumourAliquotID(String tumourAliquotID) {
		this.tumourAliquotID = tumourAliquotID;
	}

	public String getSnvPadding() {
		return this.snvPadding;
	}

	public void setSnvPadding(String snvPadding) {
		this.snvPadding = snvPadding;
	}

	public String getSvPadding() {
		return this.svPadding;
	}

	public void setSvPadding(String svPadding) {
		this.svPadding = svPadding;
	}

	public String getIndelPadding() {
		return this.indelPadding;
	}

	public void setIndelPadding(String indelPadding) {
		this.indelPadding = indelPadding;
	}

	public String getSnvVcf() {
		return this.snvVcf;
	}

	public void setSnvVcf(String snvVcf) {
		this.snvVcf = snvVcf;
	}

	public String getSvVcf() {
		return this.svVcf;
	}

	public void setSvVcf(String svVcf) {
		this.svVcf = svVcf;
	}

	public String getIndelVcf() {
		return this.indelVcf;
	}

	public void setIndelVcf(String indelVcf) {
		this.indelVcf = indelVcf;
	}

}
