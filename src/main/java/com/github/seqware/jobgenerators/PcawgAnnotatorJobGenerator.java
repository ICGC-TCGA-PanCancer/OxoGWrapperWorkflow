package com.github.seqware.jobgenerators;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import com.github.seqware.GitUtils;
import com.github.seqware.OxoGWrapperWorkflow;
import com.github.seqware.OxoGWrapperWorkflow.Pipeline;

import net.sourceforge.seqware.pipeline.workflowV2.AbstractWorkflowDataModel;
import net.sourceforge.seqware.pipeline.workflowV2.model.Job;

public class PcawgAnnotatorJobGenerator extends JobGeneratorBase {
	
	
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

	public List<Job> doAnnotations(AbstractWorkflowDataModel workflow, String tumourAliquotID, String tumourMinibamPath, String normalMinibamPath, Consumer<String> updateFilesForUpload, Job ... parents)
	{
		List<Job> annotatorJobs = new ArrayList<Job>(3);
		
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

		return annotatorJobs;
	}

	/**
	 * Runs Jonathan Dursi's Annotator tool on a input for a workflow.
	 * @param inputType - The typ of input, can be "SNV" or "indel" 
	 * @param workflowName - The name of the workflow to annotate.
	 * @param vcfPath - The path to the VCF to use as input to the annotator.
	 * @param tumourBamPath - The path to the tumour minibam (the output of variantbam).
	 * @param normalBamPath - The path to the normal minibam (the output of variantbam).
	 * @param parents - List of parent jobs.
	 * @return
	 */
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
