package com.github.seqware.jobgenerators;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

import com.github.seqware.GitUtils;
import com.github.seqware.OxoGWrapperWorkflow.BAMType;
import com.github.seqware.TemplateUtils;

import net.sourceforge.seqware.pipeline.workflowV2.AbstractWorkflowDataModel;
import net.sourceforge.seqware.pipeline.workflowV2.model.Job;

public class VariantBamJobGenerator extends JobGeneratorBase{

	@FunctionalInterface
	public interface UpdateBamForUpload<S,T,U>
	{
		public void accept(S s, T t, U u);
	}
	
	public VariantBamJobGenerator(String JSONlocation, String JSONrepoName, String JSONfolderName, String JSONfileName) {
		super(JSONlocation, JSONrepoName, JSONfolderName, JSONfileName);
	}
	

	private String aliquotID;
	private String snvPadding;
	private String svPadding;
	private String indelPadding;
	private String snvVcf;
	private String svVcf;
	private String indelVcf;
	
	public Job doVariantBam(AbstractWorkflowDataModel workflow,BAMType bamType, String bamName, String bamPath, String tumourBAMFileName, UpdateBamForUpload<String, String, Boolean> updateFilesForUpload, Job ...parents)
	{
		Job runVariantbam = workflow.getWorkflow().createBashJob("run "+bamType+(bamType == BAMType.tumour ? "_" + this.aliquotID + "_" : "")+" variantbam");

		String minibamName = "";
		//The "old" minibam names will by the names of symlinks pointing to the properly named files.
		String oldMinibamName = "";
		if (bamType == BAMType.normal)
		{
			oldMinibamName = bamName.replace(".bam", "_minibam") + ".bam";
			minibamName = this.aliquotID + ".normal.variantbam." + LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE) + ".bam";
			String normalMinibamPath = "/datastore/variantbam_results/"+minibamName;
			String oldMinibamPath = "/datastore/variantbam_results/"+oldMinibamName;
			updateFilesForUpload.accept(normalMinibamPath,null,false);
			updateFilesForUpload.accept(normalMinibamPath+".bai",null,false);
			updateFilesForUpload.accept(oldMinibamPath,null,true);
			updateFilesForUpload.accept(oldMinibamPath+".bai",null,true);
		}
		else
		{
			oldMinibamName = tumourBAMFileName.replace(".bam", "_minibam") + ".bam";
			minibamName = this.aliquotID + ".tumour.variantbam." + LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE) + ".bam";
			String tumourMinibamPath = "/datastore/variantbam_results/"+minibamName;
			String oldMinibamPath = "/datastore/variantbam_results/"+oldMinibamName;
			updateFilesForUpload.accept(tumourMinibamPath,aliquotID,false);
			updateFilesForUpload.accept(tumourMinibamPath+".bai",aliquotID,false);
			updateFilesForUpload.accept(oldMinibamPath,aliquotID,true);
			updateFilesForUpload.accept(oldMinibamPath+".bai",aliquotID,true);
		}
		
		String command = TemplateUtils.getRenderedTemplate(Arrays.stream( new String[][] {
			{ "containerNameSuffix", bamType + (bamType == BAMType.tumour ? "_with_tumour_"+this.aliquotID : "") },
			{ "minibamName", minibamName },  {"snvPadding", String.valueOf(this.snvPadding)}, {"svPadding", String.valueOf(this.svPadding)},
			{ "indelPadding", String.valueOf(this.indelPadding) }, { "pathToBam", bamPath },
			{ "snvVcf", snvVcf }, { "svVcf", svVcf }, { "indelVcf", indelVcf }
		}).collect(this.collectToMap), "runVariantbam.template" );

		String linkWithOldNamingConvention = "cd /datastore/variantbam_results && ln -sf "+minibamName+" "+oldMinibamName + " && ln -sf "+minibamName+".bai "+oldMinibamName+".bai" ;
		String moveToFailed = GitUtils.gitMoveCommand("running-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, workflow.getWorkflowBaseDir() + "/scripts/");
		command = ( "(" + command + " && " + linkWithOldNamingConvention + " ) || " + moveToFailed);
		runVariantbam.setCommand(command);

		Arrays.stream(parents).forEach(parent -> runVariantbam.addParent(parent));
		
		return runVariantbam;
	}

	public String getAliquotID() {
		return this.aliquotID;
	}

	public void setAliquotID(String aliquotID) {
		this.aliquotID = aliquotID;
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
