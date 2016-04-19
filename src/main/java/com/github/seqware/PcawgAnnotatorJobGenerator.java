package com.github.seqware;

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

	Job runAnnotator(AbstractWorkflowDataModel workflow,String inputType, Pipeline workflowName, String vcfPath, String tumourBamPath, String normalBamPath, String tumourAliquotID, Consumer<String> updateFilesForUpload, Job ...parents)
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

}
