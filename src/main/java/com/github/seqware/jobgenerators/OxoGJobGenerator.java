package com.github.seqware.jobgenerators;

import java.util.Arrays;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import com.github.seqware.GitUtils;
import com.github.seqware.TemplateUtils;
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

	private String museGnosID;
	private boolean allowMissingFiles;

	public OxoGJobGenerator(String JSONlocation, String JSONrepoName, String JSONfolderName, String JSONfileName, String tumourAliquotID, String normalAliquotID) {
		super(JSONlocation, JSONrepoName, JSONfolderName, JSONfileName);
		this.tumourAliquotID = tumourAliquotID;
		this.normalAliquotID = normalAliquotID;
	}
	
	public OxoGJobGenerator(String JSONlocation, String JSONrepoName, String JSONfolderName, String JSONfileName) {
		super(JSONlocation, JSONrepoName, JSONfolderName, JSONfileName);
	}
	
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
	public Job doOxoG(AbstractWorkflowDataModel workflow, String pathToTumour, Consumer<String> updateFilesToUpload, Job ...parents) {
		
		String moveToFailed = GitUtils.gitMoveCommand("running-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, workflow.getWorkflowBaseDir() + "/scripts/");
		Job runOxoGWorkflow = workflow.getWorkflow().createBashJob("run OxoG Filter for tumour "+this.tumourAliquotID); 
		Function<String,String> getFileName = s -> s.substring(s.lastIndexOf("/")); 
		
		String pathToResults = "/datastore/oxog_results/tumour_"+this.tumourAliquotID+"/cga/fh/pcawg_pipeline/jobResults_pipette/jobs/"+this.tumourAliquotID+"/links_for_gnos/annotate_failed_sites_to_vcfs/";
		String pathToUploadDir = "/datastore/files_for_upload/";
		String checkSangerExtractedSNV;
		String checkBroadExtractedSNV;
		String checkDkfzEmblExtractedSNV;
		BiFunction<String,Pipeline,String> getCheckExtractedSNVString = (vcf,pipeline) -> {
			if (!this.allowMissingFiles || (this.allowMissingFiles && vcf!=null && vcf.trim().length()>0))
			{
					return TemplateUtils.getRenderedTemplate(Arrays.stream(new String[][]{
						{ "extractedSNVVCF", vcf.replaceAll("/datastore/vcf/[^/]+/", "")}, { "worfklow", pipeline.toString() }, {"extractedSNVVCFPath",vcf }
					}).collect(this.collectToMap),"checkForExtractedSNVs.template");
			}
			else
			{
				return "";
			}
		};
		checkSangerExtractedSNV = getCheckExtractedSNVString.apply(extractedSangerSNV, Pipeline.sanger);
		checkBroadExtractedSNV = getCheckExtractedSNVString.apply(extractedBroadSNV, Pipeline.broad);
		checkDkfzEmblExtractedSNV = getCheckExtractedSNVString.apply(extractedDkfzEmblSNV, Pipeline.dkfz_embl);
		String runOxoGCommand = TemplateUtils.getRenderedTemplate(Arrays.stream(new String[][] {
			{ "checkSangerExtractedSNV" , checkSangerExtractedSNV },{ "checkBroadExtractedSNV" , checkBroadExtractedSNV },{ "checkDkfzEmblExtractedSNV" , checkDkfzEmblExtractedSNV },
			{ "sangerWorkflow", Pipeline.sanger.toString() }, { "broadWorkflow", Pipeline.broad.toString() },
			{ "dkfzEmblWorkflow", Pipeline.dkfz_embl.toString() }, { "museWorkflow", Pipeline.muse.toString() },
			{ "tumourID", tumourAliquotID }, { "oxoQScore", this.oxoQScore }, 
			{ "pathToTumour", pathToTumour }, { "normalBamGnosID", this.normalBamGnosID }, { "normalBAMFileName", this.normalBAMFileName } ,
			{ "broadGnosID", this.broadGnosID }, { "sangerGnosID", this.sangerGnosID }, { "dkfzemblGnosID", this.dkfzemblGnosID }, { "museGnosID", this.museGnosID },
			{ "sangerSNVName", sangerSNV}, { "broadSNVName", broadSNV }, { "dkfzEmblSNVName", dkfzEmblSNV }, { "museSNVName", museSNV },
			{ "pathToResults", pathToResults}, { "pathToUploadDir", pathToUploadDir }
		} ).collect(this.collectToMap), "runOxoGFilter.template");
		
		runOxoGWorkflow.setCommand("( "+runOxoGCommand+" ) || "+ moveToFailed);
		Arrays.stream(parents).forEach(parent -> runOxoGWorkflow.addParent(parent));
		Function<String,String> changeToOxoGSuffix = (s) ->  pathToUploadDir + s.replace(".vcf.gz", ".oxoG.vcf.gz").replaceAll("/datafiles/VCF/[^/]+/","/");
		Function<String,String> changeToOxoGTBISuffix = changeToOxoGSuffix.andThen((s) -> s+=".tbi");
		
		BiConsumer<String,Function<String,String>> addToFilesForUpload = (vcfName, vcfNameProcessor) -> 
		{
			if (this.allowMissingFiles)
			{
				if (vcfName!=null && vcfName.trim().length()>0)
				{
					updateFilesToUpload.accept(vcfNameProcessor.apply(vcfName));
				}
			}
			else
			{
				updateFilesToUpload.accept(vcfNameProcessor.apply(vcfName));
			}
		};

		//regular VCFs
		addToFilesForUpload.accept(broadSNV, changeToOxoGSuffix);
		addToFilesForUpload.accept(dkfzEmblSNV, changeToOxoGSuffix);
		addToFilesForUpload.accept(sangerSNV, changeToOxoGSuffix);
		addToFilesForUpload.accept(museSNV, changeToOxoGSuffix);
		//index files
		addToFilesForUpload.accept(broadSNV, changeToOxoGTBISuffix);
		addToFilesForUpload.accept(dkfzEmblSNV, changeToOxoGTBISuffix);
		addToFilesForUpload.accept(sangerSNV, changeToOxoGTBISuffix);
		addToFilesForUpload.accept(museSNV, changeToOxoGTBISuffix);
		//Extracted SNVs
		addToFilesForUpload.accept(extractedSangerSNV, getFileName.andThen(changeToOxoGSuffix));
		addToFilesForUpload.accept(extractedBroadSNV, getFileName.andThen(changeToOxoGSuffix));
		addToFilesForUpload.accept(extractedDkfzEmblSNV, getFileName.andThen(changeToOxoGSuffix));
		//index files
		addToFilesForUpload.accept(extractedSangerSNV, getFileName.andThen(changeToOxoGTBISuffix));
		addToFilesForUpload.accept(extractedBroadSNV, getFileName.andThen(changeToOxoGTBISuffix));
		addToFilesForUpload.accept(extractedDkfzEmblSNV, getFileName.andThen(changeToOxoGTBISuffix));
		
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

	public String getMuseGnosID() {
		return this.museGnosID;
	}

	public void setMuseGnosID(String museGnosID) {
		this.museGnosID = museGnosID;
	}

	public boolean isAllowMissingFiles() {
		return this.allowMissingFiles;
	}

	public void setAllowMissingFiles(boolean allowMissingFiles) {
		this.allowMissingFiles = allowMissingFiles;
	}
}
