package com.github.seqware;

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.Function;

import com.github.seqware.OxoGWrapperWorkflow.Pipeline;

import net.sourceforge.seqware.pipeline.workflowV2.AbstractWorkflowDataModel;
import net.sourceforge.seqware.pipeline.workflowV2.model.Job;

public class OxoGJobGenerator extends JobGeneratorBase {

	private String tumourAliquotID;
	private String normalAliquotID;

	private String extractedSangerSNV;
	private String extractedBroadSNV;
	private String extractedDkfzEmblSNV;
	private String broadGnosID;
	private String normalBamGnosID;
	private String sangerGnosID;
	private String oxoQScore;
	private String normalBAMFileName;
	private String dkfzemblGnosID;
	private String museSNV;
	private String dkfzEmblSNV;
	private String broadSNV;
	private String sangerSNV;
	private String normalizedBroadIndel;
	private String normalizedSangerIndel;
	private String normalizedDkfzEmblIndel;
	private String museGnosID;
	
	public OxoGJobGenerator(String tumourAliquotID, String normalAliquotID)
	{
		this.tumourAliquotID = tumourAliquotID;
		this.normalAliquotID = normalAliquotID;
	}
	//public OxoGJobGenerator(){}
	/**
	 * Runs the OxoG filtering program inside the Broad's OxoG docker container. Output file(s) will be in /datastore/oxog_results/tumour_${tumourAliquotID} and the working files will 
	 * be in /datastore/oxog_workspace/tumour_${tumourAliquotID}
	 * @param parent
	 * @return
	 */
	Job doOxoG(AbstractWorkflowDataModel workflow, String pathToTumour, Consumer<String> updateFilesToUpload, Job ...parents) {
		
		String moveToFailed = GitUtils.gitMoveCommand("running-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, workflow.getWorkflowBaseDir() + "/scripts/");
		Job runOxoGWorkflow = workflow.getWorkflow().createBashJob("run OxoG Filter for tumour "+this.tumourAliquotID); 
		Function<String,String> getFileName = (s) -> {  return s.substring(s.lastIndexOf("/")); };
		String pathToResults = "/datastore/oxog_results/tumour_"+this.tumourAliquotID+"/cga/fh/pcawg_pipeline/jobResults_pipette/jobs/"+this.normalAliquotID+"/links_for_gnos/annotate_failed_sites_to_vcfs/";
		String pathToUploadDir = "/datastore/files_for_upload/";
		
		String runOxoGCommand = TemplateUtils.getRenderedTemplate(Arrays.stream(new String[][] {
				{ "sangerExtractedSNVVCFPath", extractedSangerSNV }, { "sangerWorkflow", Pipeline.sanger.toString() }, { "sangerExtractedSNVVCF", getFileName.apply(extractedSangerSNV) },
				{ "broadExtractedSNVVCFPath", extractedBroadSNV }, { "broadWorkflow", Pipeline.broad.toString() }, { "broadExtractedSNVVCF", getFileName.apply(extractedBroadSNV) },
				{ "dkfzEmblExtractedSNVVCFPath", extractedDkfzEmblSNV }, { "dkfzEmblWorkflow", Pipeline.dkfz_embl.toString() }, { "dkfzEmblExtractedSNVVCF", getFileName.apply(extractedDkfzEmblSNV) },
				{ "tumourID", tumourAliquotID }, { "aliquotID", this.normalAliquotID }, { "oxoQScore", this.oxoQScore }, { "museWorkflow", Pipeline.muse.toString() },
				{ "pathToTumour", pathToTumour }, { "normalBamGnosID", this.normalBamGnosID }, { "normalBAMFileName", this.normalBAMFileName } ,
				{ "broadGnosID", this.broadGnosID }, { "sangerGnosID", this.sangerGnosID }, { "dkfzemblGnosID", this.dkfzemblGnosID }, { "museGnosID", this.museGnosID },
				{ "sangerSNVName", sangerSNV}, { "broadSNVName", broadSNV }, { "dkfzEmblSNVName", dkfzEmblSNV }, { "museSNVName", museSNV },
				{ "pathToResults", pathToResults}, { "pathToUploadDir", pathToUploadDir },
				{ "broadNormalizedIndelVCFName",normalizedBroadIndel }, { "sangerNormalizedIndelVCFName",normalizedSangerIndel },
				{ "dkfzEmblNormalizedIndelVCFName",normalizedDkfzEmblIndel }
			} ).collect(this.collectToMap), "runOxoGFilter.template");
		runOxoGWorkflow.setCommand("( "+runOxoGCommand+" ) || "+ moveToFailed);
		for (Job parent : parents)
		{
			runOxoGWorkflow.addParent(parent);
		}
		
		Function<String,String> changeToOxoGSuffix = (s) -> {return pathToUploadDir + s.replace(".vcf.gz", ".oxoG.vcf.gz"); };
		Function<String,String> changeToOxoGTBISuffix = changeToOxoGSuffix.andThen((s) -> s+=".tbi"); 
		//regular VCFs
		updateFilesToUpload.accept(changeToOxoGSuffix.apply(broadSNV)); //this.filesForUpload.add(changeToOxoGSuffix.apply(broadSNV));
		updateFilesToUpload.accept(changeToOxoGSuffix.apply(dkfzEmblSNV));
		updateFilesToUpload.accept(changeToOxoGSuffix.apply(sangerSNV));
		updateFilesToUpload.accept(changeToOxoGSuffix.apply(museSNV));
		//index files
		updateFilesToUpload.accept(changeToOxoGTBISuffix.apply(broadSNV));
		updateFilesToUpload.accept(changeToOxoGTBISuffix.apply(dkfzEmblSNV));
		updateFilesToUpload.accept(changeToOxoGTBISuffix.apply(sangerSNV));
		updateFilesToUpload.accept(changeToOxoGTBISuffix.apply(museSNV));
		//Extracted SNVs
		updateFilesToUpload.accept(changeToOxoGSuffix.apply(getFileName.apply(extractedSangerSNV)));
		updateFilesToUpload.accept(changeToOxoGSuffix.apply(getFileName.apply(extractedBroadSNV)));
		updateFilesToUpload.accept(changeToOxoGSuffix.apply(getFileName.apply(extractedDkfzEmblSNV)));
		//index files
		updateFilesToUpload.accept(changeToOxoGTBISuffix.apply(getFileName.apply(extractedSangerSNV)));
		updateFilesToUpload.accept(changeToOxoGTBISuffix.apply(getFileName.apply(extractedBroadSNV)));
		updateFilesToUpload.accept(changeToOxoGTBISuffix.apply(getFileName.apply(extractedDkfzEmblSNV)));
		
		updateFilesToUpload.accept("/datastore/files_for_upload/" + this.normalAliquotID + ".gnos_files_tumour_" + this.tumourAliquotID + ".tar");
		updateFilesToUpload.accept("/datastore/files_for_upload/" + this.normalAliquotID + ".call_stats_tumour_" + this.tumourAliquotID + ".txt.gz.tar");
		
		return runOxoGWorkflow;
		
	}

	public String getExtractedSangerSNV() {
		return this.extractedSangerSNV;
	}

	public void setExtractedSangerSNV(String extractedSangerSNV) {
		this.extractedSangerSNV = extractedSangerSNV;
	}

	public String getExtractedBroadSNV() {
		return this.extractedBroadSNV;
	}

	public void setExtractedBroadSNV(String extractedBroadSNV) {
		this.extractedBroadSNV = extractedBroadSNV;
	}

	public String getExtractedDkfzEmblSNV() {
		return this.extractedDkfzEmblSNV;
	}

	public void setExtractedDkfzEmblSNV(String extractedDkfzEmblSNV) {
		this.extractedDkfzEmblSNV = extractedDkfzEmblSNV;
	}

	public String getBroadGnosID() {
		return this.broadGnosID;
	}

	public void setBroadGnosID(String broadGnosID) {
		this.broadGnosID = broadGnosID;
	}

	public String getNormalBamGnosID() {
		return this.normalBamGnosID;
	}

	public void setNormalBamGnosID(String normalBamGnosID) {
		this.normalBamGnosID = normalBamGnosID;
	}

	public String getSangerGnosID() {
		return this.sangerGnosID;
	}

	public void setSangerGnosID(String sangerGnosID) {
		this.sangerGnosID = sangerGnosID;
	}

	public String getOxoQScore() {
		return this.oxoQScore;
	}

	public void setOxoQScore(String oxoQScore) {
		this.oxoQScore = oxoQScore;
	}

	public String getNormalBAMFileName() {
		return this.normalBAMFileName;
	}

	public void setNormalBAMFileName(String normalBAMFileName) {
		this.normalBAMFileName = normalBAMFileName;
	}

	public String getDkfzemblGnosID() {
		return this.dkfzemblGnosID;
	}

	public void setDkfzemblGnosID(String dkfzemblGnosID) {
		this.dkfzemblGnosID = dkfzemblGnosID;
	}

	public String getMuseSNV() {
		return this.museSNV;
	}

	public void setMuseSNV(String museSNV) {
		this.museSNV = museSNV;
	}

	public String getDkfzEmblSNV() {
		return this.dkfzEmblSNV;
	}

	public void setDkfzEmblSNV(String dkfzEmblSNV) {
		this.dkfzEmblSNV = dkfzEmblSNV;
	}

	public String getBroadSNV() {
		return this.broadSNV;
	}

	public void setBroadSNV(String broadSNV) {
		this.broadSNV = broadSNV;
	}

	public String getSangerSNV() {
		return this.sangerSNV;
	}

	public void setSangerSNV(String sangerSNV) {
		this.sangerSNV = sangerSNV;
	}

	public String getNormalizedBroadIndel() {
		return this.normalizedBroadIndel;
	}

	public void setNormalizedBroadIndel(String normalizedBroadIndel) {
		this.normalizedBroadIndel = normalizedBroadIndel;
	}

	public String getNormalizedSangerIndel() {
		return this.normalizedSangerIndel;
	}

	public void setNormalizedSangerIndel(String normalizedSangerIndel) {
		this.normalizedSangerIndel = normalizedSangerIndel;
	}

	public String getNormalizedDkfzEmblIndel() {
		return this.normalizedDkfzEmblIndel;
	}

	public void setNormalizedDkfzEmblIndel(String normalizedDkfzEmblIndel) {
		this.normalizedDkfzEmblIndel = normalizedDkfzEmblIndel;
	}

	public String getMuseGnosID() {
		return this.museGnosID;
	}

	public void setMuseGnosID(String museGnosID) {
		this.museGnosID = museGnosID;
	}
}
