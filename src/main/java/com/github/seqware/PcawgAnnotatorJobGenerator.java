package com.github.seqware;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import com.github.seqware.OxoGWrapperWorkflow.Pipeline;

import net.sourceforge.seqware.pipeline.workflowV2.AbstractWorkflowDataModel;
import net.sourceforge.seqware.pipeline.workflowV2.model.Job;

public class PcawgAnnotatorJobGenerator {
	
	
	private String JSONlocation;
	private String JSONrepoName;
	private String JSONfolderName;
	private String JSONfileName;
	private boolean gitMoveTestMode;
	private String normalizedBroadIndel;
	private String normalizedDkfzEmblIndel;
	private String normalizedSangerIndel;
	private String broadOxogSNVFileName;
	private String dkfzEmbleOxogSNVFileName;
	private String sangerOxogSNVFileName;
	private String museOxogSNVFileName;
	private String broadOxoGSNVFromIndelFileName;
	private String dkfzEmblOxoGSNVFromIndelFileName;
	private String sangerOxoGSNVFromIndelFileName;

	List<Job> doAnnotations(AbstractWorkflowDataModel workflow, String tumourAliquotID, String tumourMinibamPath, String normalMinibamPath, Consumer<String> updateFilesForUpload, Job ... parents)
	{
		List<Job> annotatorJobs = new ArrayList<Job>(3);
		
//		Predicate<String> isExtractedSNV = p -> p.contains("extracted-snv") && p.endsWith(".vcf.gz");
//		final String passFilteredOxoGSuffix = ".pass-filtered.oxoG.vcf.gz";
		//list filtering should only ever produce one result.
		
//		for (int i = 0; i < this.tumours.size(); i++)
		{
//			TumourInfo tInf = this.tumours.get(i);
//			String tumourAliquotID = tInf.getAliquotID();
//			String broadOxogSNVFileName = this.filesForUpload.stream().filter(p -> ((p.contains(tumourAliquotID) && p.contains("broad-mutect") && p.endsWith(passFilteredOxoGSuffix)))).findFirst().orElseGet(emptyStringWhenMissingFilesAllowed);
//			String broadOxoGSNVFromIndelFileName = this.filesForUpload.stream().filter(p -> (p.contains(Pipeline.broad.toString()) && isExtractedSNV.test(p) )).findFirst().orElseGet(emptyStringWhenMissingFilesAllowed);
//			
//			
//			String sangerOxogSNVFileName = this.filesForUpload.stream().filter(p -> ((p.contains(tumourAliquotID) && p.contains("svcp_") && p.endsWith(passFilteredOxoGSuffix)))).findFirst().orElseGet(emptyStringWhenMissingFilesAllowed);
//			String sangerOxoGSNVFromIndelFileName = this.filesForUpload.stream().filter(p -> (p.contains(Pipeline.sanger.toString()) && isExtractedSNV.test(p) )).findFirst().orElseGet(emptyStringWhenMissingFilesAllowed);
//			
//			String dkfzEmbleOxogSNVFileName = this.filesForUpload.stream().filter(p -> ((p.contains(tumourAliquotID) && p.contains("dkfz-snvCalling") && p.endsWith(passFilteredOxoGSuffix)))).findFirst().orElseGet(emptyStringWhenMissingFilesAllowed);
//			String dkfzEmblOxoGSNVFromIndelFileName = this.filesForUpload.stream().filter(p -> (p.contains(Pipeline.dkfz_embl.toString()) && isExtractedSNV.test(p) )).findFirst().orElseGet(emptyStringWhenMissingFilesAllowed);
//
//			//Remember: MUSE files do not get PASS-filtered. Also, there is no INDEL so there cannot be any SNVs extracted from INDELs.
//			String museOxogSNVFileName = this.filesForUpload.stream().filter(p -> p.toUpperCase().contains("MUSE") && p.endsWith(".oxoG.vcf.gz")).findFirst().orElseGet(emptyStringWhenMissingFilesAllowed);
//			
//			String normalizedBroadIndel = this.normalizedIndels.stream().filter(isBroad.and(matchesTumour(tumourAliquotID))).findFirst().get().getFileName();
//			String normalizedSangerIndel = this.normalizedIndels.stream().filter(isSanger.and(matchesTumour(tumourAliquotID))).findFirst().get().getFileName();
//			String normalizedDkfzEmblIndel = this.normalizedIndels.stream().filter(isDkfzEmbl.and(matchesTumour(tumourAliquotID))).findFirst().get().getFileName();
			
			Job broadIndelAnnotatorJob = this.runAnnotator(workflow,"indel", Pipeline.broad, normalizedBroadIndel, tumourMinibamPath, normalMinibamPath, tumourAliquotID, updateFilesForUpload, parents);
			Job dfkzEmblIndelAnnotatorJob = this.runAnnotator(workflow,"indel", Pipeline.dkfz_embl, normalizedDkfzEmblIndel, tumourMinibamPath, normalMinibamPath, tumourAliquotID, updateFilesForUpload, broadIndelAnnotatorJob);
			Job sangerIndelAnnotatorJob = this.runAnnotator(workflow, "indel", Pipeline.sanger, normalizedSangerIndel, tumourMinibamPath, normalMinibamPath, tumourAliquotID,updateFilesForUpload, dfkzEmblIndelAnnotatorJob);
	
			Job broadSNVAnnotatorJob = this.runAnnotator(workflow,"SNV", Pipeline.broad,broadOxogSNVFileName, tumourMinibamPath, normalMinibamPath, tumourAliquotID,updateFilesForUpload, parents);
			Job dfkzEmblSNVAnnotatorJob = this.runAnnotator(workflow,"SNV", Pipeline.dkfz_embl,dkfzEmbleOxogSNVFileName, tumourMinibamPath, normalMinibamPath, tumourAliquotID,updateFilesForUpload, broadSNVAnnotatorJob);
			Job sangerSNVAnnotatorJob = this.runAnnotator(workflow,"SNV",Pipeline.sanger,sangerOxogSNVFileName, tumourMinibamPath, normalMinibamPath, tumourAliquotID,updateFilesForUpload, dfkzEmblSNVAnnotatorJob);
			Job museSNVAnnotatorJob = this.runAnnotator(workflow,"SNV",Pipeline.muse,museOxogSNVFileName, tumourMinibamPath, normalMinibamPath, tumourAliquotID,updateFilesForUpload, dfkzEmblSNVAnnotatorJob);
	
			Job broadSNVFromIndelAnnotatorJob = this.runAnnotator(workflow,"SNV",Pipeline.broad, broadOxoGSNVFromIndelFileName, tumourMinibamPath, normalMinibamPath, tumourAliquotID,updateFilesForUpload, parents);
			Job dfkzEmblSNVFromIndelAnnotatorJob = this.runAnnotator(workflow,"SNV",Pipeline.dkfz_embl, dkfzEmblOxoGSNVFromIndelFileName, tumourMinibamPath, normalMinibamPath, tumourAliquotID,updateFilesForUpload, broadSNVFromIndelAnnotatorJob);
			Job sangerSNVFromIndelAnnotatorJob = this.runAnnotator(workflow,"SNV",Pipeline.sanger, sangerOxoGSNVFromIndelFileName, tumourMinibamPath, normalMinibamPath, tumourAliquotID,updateFilesForUpload, dfkzEmblSNVFromIndelAnnotatorJob);

			annotatorJobs.add(sangerSNVFromIndelAnnotatorJob);
			annotatorJobs.add(sangerSNVAnnotatorJob);
			annotatorJobs.add(sangerIndelAnnotatorJob);
			annotatorJobs.add(museSNVAnnotatorJob);
		}
		return annotatorJobs;
	}

	
	Job runAnnotator(AbstractWorkflowDataModel workflow, String inputType, Pipeline workflowName, String vcfPath, String tumourBamPath, String normalBamPath, String tumourAliquotID, Consumer<String> updateFilesForUpload, Job ...parents)
	{
		String outDir = "/datastore/files_for_upload/";
		String containerName = "pcawg-annotator_"+workflowName+"_"+inputType;
		String commandName ="run annotator for "+workflowName+" "+inputType;
		String annotatedFileName = tumourAliquotID+"_annotated_"+workflowName+"_"+inputType+".vcf";
		//If a filepath contains the phrase "extracted" then it contains SNVs that were extracted from an INDEL.
		if (vcfPath.contains("extracted"))
		{
			containerName += "_SNVs-from-INDELs";
			commandName += "_SNVs-from-INDELs";
			annotatedFileName = annotatedFileName.replace(inputType, "SNVs_from_INDELs");
		}
		String command = "";
		
		containerName += "_tumour_"+tumourAliquotID;
		commandName += "_tumour_"+tumourAliquotID;
		Job annotatorJob = workflow.getWorkflow().createBashJob(commandName);
//		if (!this.skipAnnotation)
		{
			// The "[ -f ${vcfPath} ]..." exists to handle cases where there is an SNV extracted from an INDEL.
			// We have to have a condition that checks at script-run time because we don't know if it will exist 
			// when the workflow engine first builds the scripts.
			// Also, the call to the ljdursi/pcawg-annotate container looks a little weird, (inside parens and redirected to a file),
			// but that seems to be the easiest way to get the capture the outpuyt from it. 
			command = "(\n"
					+ "if [ -f "+vcfPath+" ] ; then \n"
					+ "    (docker run --rm --name="+containerName+" -v "+vcfPath+":/input.vcf \\\n"
					+ "        -v "+tumourBamPath+":/tumour_minibam.bam \\\n"
					+ "        -v "+normalBamPath+":/normal_minibam.bam \\\n"
					+ "        ljdursi/pcawg-annotate \\\n"
					+ "        "+inputType+" /input.vcf /normal_minibam.bam /tumour_minibam.bam ) > "+outDir+"/"+annotatedFileName+" \n"
					+ "    bgzip -f -c "+outDir+"/"+annotatedFileName+" > "+outDir+"/"+annotatedFileName+".gz \n"
					+ "    tabix -p vcf "+outDir+"/"+annotatedFileName+".gz \n "
					+ "fi\n"
					+ ") " ;
			
			
			String moveToFailed = GitUtils.gitMoveCommand("running-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, workflow.getWorkflowBaseDir() + "/scripts/");
			command += " || " + moveToFailed;
		}
		updateFilesForUpload.accept("/datastore/files_for_upload/"+annotatedFileName+".gz ");
		updateFilesForUpload.accept("/datastore/files_for_upload/"+annotatedFileName+".gz.tbi ");
		
		annotatorJob.setCommand(command);
		for (Job parent : parents)
		{
			annotatorJob.addParent(parent);
		}
		return annotatorJob;
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


	public String getNormalizedBroadIndel() {
		return this.normalizedBroadIndel;
	}


	public void setNormalizedBroadIndel(String normalizedBroadIndel) {
		this.normalizedBroadIndel = normalizedBroadIndel;
	}


	public String getNormalizedDkfzEmblIndel() {
		return this.normalizedDkfzEmblIndel;
	}


	public void setNormalizedDkfzEmblIndel(String normalizedDkfzEmblIndel) {
		this.normalizedDkfzEmblIndel = normalizedDkfzEmblIndel;
	}


	public String getNormalizedSangerIndel() {
		return this.normalizedSangerIndel;
	}


	public void setNormalizedSangerIndel(String normalizedSangerIndel) {
		this.normalizedSangerIndel = normalizedSangerIndel;
	}


	public String getBroadOxogSNVFileName() {
		return this.broadOxogSNVFileName;
	}


	public void setBroadOxogSNVFileName(String broadOxogSNVFileName) {
		this.broadOxogSNVFileName = broadOxogSNVFileName;
	}


	public String getDkfzEmbleOxogSNVFileName() {
		return this.dkfzEmbleOxogSNVFileName;
	}


	public void setDkfzEmbleOxogSNVFileName(String dkfzEmbleOxogSNVFileName) {
		this.dkfzEmbleOxogSNVFileName = dkfzEmbleOxogSNVFileName;
	}


	public String getSangerOxogSNVFileName() {
		return this.sangerOxogSNVFileName;
	}


	public void setSangerOxogSNVFileName(String sangerOxogSNVFileName) {
		this.sangerOxogSNVFileName = sangerOxogSNVFileName;
	}


	public String getMuseOxogSNVFileName() {
		return this.museOxogSNVFileName;
	}


	public void setMuseOxogSNVFileName(String museOxogSNVFileName) {
		this.museOxogSNVFileName = museOxogSNVFileName;
	}


	public String getBroadOxoGSNVFromIndelFileName() {
		return this.broadOxoGSNVFromIndelFileName;
	}


	public void setBroadOxoGSNVFromIndelFileName(String broadOxoGSNVFromIndelFileName) {
		this.broadOxoGSNVFromIndelFileName = broadOxoGSNVFromIndelFileName;
	}


	public String getDkfzEmblOxoGSNVFromIndelFileName() {
		return this.dkfzEmblOxoGSNVFromIndelFileName;
	}


	public void setDkfzEmblOxoGSNVFromIndelFileName(String dkfzEmblOxoGSNVFromIndelFileName) {
		this.dkfzEmblOxoGSNVFromIndelFileName = dkfzEmblOxoGSNVFromIndelFileName;
	}


	public String getSangerOxoGSNVFromIndelFileName() {
		return this.sangerOxoGSNVFromIndelFileName;
	}


	public void setSangerOxoGSNVFromIndelFileName(String sangerOxoGSNVFromIndelFileName) {
		this.sangerOxoGSNVFromIndelFileName = sangerOxoGSNVFromIndelFileName;
	}

}
