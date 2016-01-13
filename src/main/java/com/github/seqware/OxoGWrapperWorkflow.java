package com.github.seqware;

import java.util.Arrays;
import java.util.Collection;

import net.sourceforge.seqware.pipeline.workflowV2.AbstractWorkflowDataModel;
import net.sourceforge.seqware.pipeline.workflowV2.model.Job;

public class OxoGWrapperWorkflow extends AbstractWorkflowDataModel {

	private String oxoQScore = "";
	// private String donorID;
	private String aliquotID;
	private String bamNormalObjectID;
	private String bamTumourObjectID;
	private String sangerVCFObjectID;
	private String dkfzemblVCFObjectID;
	private String broadVCFObjectID;
	private String uploadURL;

	private String JSONrepo = null;
	private String JSONrepoName = "oxog-ops";
	private String JSONfolderName = null;
	private String JSONlocation = "/datastore/gitroot";
	private String JSONfileName = null;

	private String GITemail = "";
	private String GITname = "ICGC AUTOMATION";
	private String GITPemFile = "";

	private String tumourBAM;
	private String normalBAM;
	private String sangerVCF;
	private String dkfzEmblVCF;
	private String broadVCF;
	
	private boolean gitMoveTestMode = false;

	private String getMandatoryProperty(String propName) throws Exception
	{
		if (hasPropertyAndNotNull(propName)) {
			return getProperty(propName);
		}
		else {
			throw new Exception ("Property with key "+propName+ " cannot be null!");
		}
	}
	
	private void init() {
		try {
			
			this.oxoQScore = this.getMandatoryProperty(JSONUtils.OXOQ_SCORE);
			this.JSONrepo = this.getMandatoryProperty("JSONrepo");
			this.JSONrepoName = this.getMandatoryProperty("JSONrepoName");
			this.JSONfolderName = this.getMandatoryProperty("JSONfolderName");
			this.JSONfileName = this.getMandatoryProperty("JSONfileName");
			this.GITemail = this.getMandatoryProperty("GITemail");
			this.GITname = this.getMandatoryProperty("GITname");
			
			this.bamNormalObjectID = this.getMandatoryProperty(JSONUtils.BAM_NORMAL_OBJECT_ID);
			this.bamTumourObjectID = this.getMandatoryProperty(JSONUtils.BAM_TUMOUR_OBJECT_ID);
			this.sangerVCFObjectID = this.getMandatoryProperty(JSONUtils.SANGER_VCF_OBJECT_ID);
			this.dkfzemblVCFObjectID = this.getMandatoryProperty(JSONUtils.DKFZEMBL_VCF_OBJECT_ID);
			this.broadVCFObjectID = this.getMandatoryProperty(JSONUtils.BROAD_VCF_OBJECT_ID);
			this.uploadURL = this.getMandatoryProperty("uploadURL");
			this.aliquotID = this.getMandatoryProperty(JSONUtils.ALIQUOT_ID);
			
			this.GITPemFile = this.getMandatoryProperty("GITPemFile");

			if (hasPropertyAndNotNull("gitMoveTestMode")) {
				//gitMoveTestMode is not mandatory - it should default to false.
				this.gitMoveTestMode = Boolean.valueOf(getProperty("gitMoveTestMode"));
			}
			
		} catch (Exception e) {
			throw new RuntimeException("Exception encountered during workflow init: "+e.getMessage(),e);
		}
	}


	private Job getBAMs(Job parentJob) {
		Job getNormalBamFileJob = this.getWorkflow().createBashJob("get Normal BAM file");
		getNormalBamFileJob.addParent(parentJob);
		String storageClientDockerCmdNormal ="docker run --rm"
					+ " -e STORAGE_PROFILE=collab "
				    + " -v /datastore/bam/normal/logs/:/icgc/icgc-storage-client/logs/:rw "
					+ " -v /home/ubuntu/.gnos/collab.token:/icgc/icgc-storage-client/conf/application.properties:ro "
				    + " -v /datastore/bam/normal:/downloads/:rw"
				    + " icgc/icgc-storage-client "
					+ " /icgc/icgc-storage-client/bin/icgc-storage-client download --object-id "
						+ this.bamNormalObjectID+" --output-dir /downloads/";
		getNormalBamFileJob.setCommand(storageClientDockerCmdNormal);
		this.normalBAM = "/datastore/bam/normal/*.bam";
		
		
		Job getTumourBamFileJob = this.getWorkflow().createBashJob("get Tumour BAM file");
		getTumourBamFileJob.addParent(getNormalBamFileJob);
		String storageClientDockerCmdTumour = "docker run --rm"
					+ " -e STORAGE_PROFILE=collab "
				    + " -v /datastore/bam/tumour/logs/:/icgc/icgc-storage-client/logs/:rw "
					+ " -v /home/ubuntu/.gnos/collab.token:/icgc/icgc-storage-client/conf/application.properties:ro "
				    + " -v /datastore/bam/tumour:/downloads/:rw"
				    + " icgc/icgc-storage-client "
					+ " /icgc/icgc-storage-client/bin/icgc-storage-client download --object-id "
						+ this.bamTumourObjectID+" --output-dir /downloads/";
		getTumourBamFileJob.setCommand(storageClientDockerCmdTumour);
		this.tumourBAM = "/datastore/bam/tumour/*.bam";
		
		return getTumourBamFileJob;
	}

	// This will download VCFs for a workflow, based on an object ID. It will also perform the VCF Primitives
	// operation on the indel VCF and then do VCFCombine on
	// the indel, sv, and snv VCFs.
	private Job getVCF(Job parentJob, String workflowName, String objectID) {
		Job getVCFJob = this.getWorkflow().createBashJob("get VCF for workflow " + workflowName);
		String outDir = "/datastore/vcf/"+workflowName;
		String getVCFCommand = "docker run --rm"
				+ " -e STORAGE_PROFILE=collab "
			    + " -v /datastore/bam/tumour/logs/:/icgc/icgc-storage-client/logs/:rw "
				+ " -v /home/ubuntu/.gnos/collab.token:/icgc/icgc-storage-client/conf/application.properties:ro "
			    + " -v "+outDir+"/:/downloads/:rw"
	    		+ " icgc/icgc-storage-client "
				+ " /icgc/icgc-storage-client/bin/icgc-storage-client download --object-id " + objectID+" --output-dir /downloads/";
		getVCFJob.setCommand(getVCFCommand);
		getVCFJob.addParent(parentJob);

		Job unzip = this.getWorkflow().createBashJob("unzip VCFs");
		unzip.setCommand("gunzip "+outDir+"/*.vcf.gz");
		unzip.addParent(getVCFJob);
		
		Job vcfPrimitivesJob = this.getWorkflow().createBashJob("run VCF primitives on indel");
		String runVCFPrimitivesCommand = " docker run --rm " +
										" -v "+outDir+"/*.somatic.indel.vcf:/datastore/datafile.vcf " +
										" compbio/ngseasy-base:a1.0-002 vcfallelicprimitives /datastore/datafile.vcf  "+
									" > "+outDir+"/somatic.indel.PRIMITIVES.vcf";
		vcfPrimitivesJob.setCommand(runVCFPrimitivesCommand);
		vcfPrimitivesJob.addParent(unzip);
		
		Job vcfCombineJob = this.getWorkflow().createBashJob("run VCF Combine on VCFs for workflow");
		String runVCFCombineCommand = " docker run --rm "+
									  " -v "+outDir+"/:/VCFs/"+
									  " compbio/ngseasy-base:a1.0-002 vcfcombine /VCFs/*somatic.snv_mnv.vcf /VCFs/*somatic.indel.PRIMITIVES.vcf /VCFs/*somatic.sv.vcf" +
								  " > "+outDir+"/snv_AND_indel_AND_sv.vcf";
		vcfCombineJob.setCommand(runVCFCombineCommand);
		vcfCombineJob.addParent(vcfPrimitivesJob);
		
		if (workflowName.equals("Sanger"))
			this.sangerVCF = outDir + "/snv_AND_indel_AND_sv.vcf";
		else if (workflowName.equals("DKFZ_EMBL"))
			this.dkfzEmblVCF = outDir + "/snv_AND_indel_AND_sv.vcf";
		else if (workflowName.equals("Broad"))
			this.broadVCF = outDir + "/snv_AND_indel_AND_sv.vcf";

		return vcfCombineJob;
	}

	private Job doOxoG(Collection<Job> parents) {
		Job runOxoGWorkflow = this.getWorkflow().createBashJob("Run OxoG");
		String oxogMounts = " -v /datastore/refdata/:/cga/fh/pcawg_pipeline/refdata/ "+
          " -v /datastore/oncotator_db/:/cga/fh/pcawg_pipeline/refdata/public/oncotator_db/ " + 
          " -v /datastore/oxog_workspace/:/cga/fh/pcawg_pipeline/jobResults_pipette/jobs/"+this.aliquotID+"/:rw " +
          " -v /datastore/bam/:/datafiles/BAM/  -v /datastore/vcf/:/datafiles/VCF/ "+
          " -v /datastore/oxog_results/:/cga/fh/pcawg_pipeline/jobResults_pipette/results:rw ";
		String oxogCommand = "/cga/fh/pcawg_pipeline/pipelines/run_one_pipeline.bash pcawg /cga/fh/pcawg_pipeline/pipelines/oxog_pipeline.py "
				+ this.aliquotID + " " + this.tumourBAM + " " + this.normalBAM + " " + this.oxoQScore + " "
				+ this.sangerVCF + " " + this.dkfzEmblVCF + " " + this.broadVCF;
		runOxoGWorkflow.setCommand(
				"docker run --name=\"oxog_container\" "+oxogMounts+" oxog " + oxogCommand);
		// Running OxoG has multiple parents. Each download-input-file job is a
		// parent and this can only run when they are all done.
		// Running all downloads in parallel could be useful if they don't all download at maximum speed.
		// Also, the download-input jobs will do the PRIMITIVES and Combine processing on the VCFs and that
		// can definitely be done in parallel (per workflow).
		for (Job j : parents) {
			runOxoGWorkflow.addParent(j);
		}

		Job getLogs = this.getOxoGLogs(runOxoGWorkflow);

		return getLogs;
	}

	private Job getOxoGLogs(Job parent) {
		Job getLog = this.getWorkflow().createBashJob("cat OxoG docker logs");
		// This will get the docker logs, but we may also want to get the logs
		// in the mounted oxog_workspace dir...
		getLog.setCommand("docker logs oxog_run");
		getLog.addParent(parent);
		return getLog;
	}

	private Job doUpload(Job parentJob) {
		// Might need to run gtupload to generate the analysis.xml and manifest files (but not actually upload). 
		// The tar file contains all results.
		Job uploadResults = this.getWorkflow().createBashJob("upload results");
		uploadResults.setCommand("rsync /cga/fh/pcawg_pipeline/jobResults_pipette/results/" + this.aliquotID
				+ ".oxoG.somatic.snv_mnv.vcf.gz.tar  " + this.uploadURL);
		uploadResults.addParent(parentJob);
		return uploadResults;
	}
	
	private Job pullRepo() {
		Job pullRepoJob = this.getWorkflow().createBashJob("pull_git_repo");
		pullRepoJob.getCommand().addArgument("if [[ ! -d ~/.ssh/ ]]; then  mkdir ~/.ssh; fi \n");
		pullRepoJob.getCommand().addArgument("cp " + this.GITPemFile + " ~/.ssh/id_rsa \n");
		pullRepoJob.getCommand().addArgument("chmod 600 ~/.ssh/id_rsa \n");
		pullRepoJob.getCommand().addArgument("echo 'StrictHostKeyChecking no' > ~/.ssh/config \n");
		pullRepoJob.getCommand().addArgument("[ -d "+this.JSONlocation+" ] || mkdir -p "+this.JSONlocation+" \n");
		pullRepoJob.getCommand().addArgument("cd " + this.JSONlocation + " \n");
		pullRepoJob.getCommand().addArgument("git config --global user.name " + this.GITname + " \n");
		pullRepoJob.getCommand().addArgument("git config --global user.email " + this.GITemail + " \n");
		pullRepoJob.getCommand().addArgument("[ -d "+this.JSONlocation+"/"+this.JSONrepoName+" ] || git clone " + this.JSONrepo + " \n");
		//pullRepoJob.getCommand().addArgument("echo $? \n");
		pullRepoJob.getCommand().addArgument("echo \"contents: \"\n");
		pullRepoJob.getCommand().addArgument("ls -la  \n");
		return pullRepoJob;
	}

	private Job gitMove(Job lastJob, String src, String dst) {
		Job manageGit = this.getWorkflow().createBashJob("git_manage_" + src + "_" + dst);
		String path = this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName;
		// It shouldn't be necessary to do this config again if it was already done in pullRepo, but probably safer this way.
		manageGit.getCommand().addArgument("git config --global user.name " + this.GITname + " \n");
		manageGit.getCommand().addArgument("git config --global user.email " + this.GITemail + " \n");
		// I think maybe it should be an error if the *repo* doesn't exist.
		manageGit.getCommand().addArgument("cd "+this.JSONlocation +"/" + this.JSONrepoName + " \n");
		manageGit.getCommand().addArgument("[ -d "+path+" ] || mkdir -p "+path+" \n");
		manageGit.getCommand().addArgument("cd " + path + " \n");
		manageGit.getCommand().addArgument("# This is not idempotent: git pull \n");

		// If gitMoveTestMode is true, then the file moves will only happen locally, but will not be checked into git. 
		if (!gitMoveTestMode) {
			manageGit.getCommand().addArgument("git checkout master \n");
			manageGit.getCommand().addArgument("git reset --hard origin/master \n");
			manageGit.getCommand().addArgument("git fetch --all \n");
		}
		manageGit.getCommand().addArgument("[ -d "+dst+" ] || mkdir -p "+dst+" \n");
		
		if (!gitMoveTestMode) {
			manageGit.getCommand().addArgument("if [[ -d " + src + " ]]; then git mv " + path + "/" + src + "/"
					+ this.JSONfileName + " " + path + "/" + dst + "; fi \n");
			manageGit.getCommand().addArgument("git stage . \n");
			manageGit.getCommand().addArgument("git commit -m '" + dst + ": " + this.JSONfileName + "' \n");
			manageGit.getCommand().addArgument("git push \n");
		}
		else {
			manageGit.getCommand().addArgument("if [[ -d " + src + " ]]; then mv " + path + "/" + src + "/"
					+ this.JSONfileName + " " + path + "/" + dst + "; fi \n");
		}
		manageGit.addParent(lastJob);
		return manageGit;
	}

	@Override
	public void buildWorkflow() {
		this.init();
		// Pull the repo.
		Job pullRepo = this.pullRepo();
		// indicate job is in downloading stage.
		Job move2download = gitMove(pullRepo, "queued-jobs", "downloading-jobs");
		Job bamJob = this.getBAMs(move2download);
		Job sangerVCFJob = this.getVCF(move2download, "Sanger", this.sangerVCFObjectID);
		Job dkfzEmblVCFJob = this.getVCF(move2download, "DKFZ_EMBL", this.dkfzemblVCFObjectID);
		Job broadVCFJob = this.getVCF(move2download, "Broad", this.broadVCFObjectID);

		// indicate job is running.
		//Note: because of the way that addParent works, we need to pass at least ONE parent
		//object here, and then add the rest in a loop below.
		Job move2running = gitMove(bamJob, "queued-jobs", "running-jobs");
		for (Job j : Arrays.asList(sangerVCFJob, dkfzEmblVCFJob, broadVCFJob)) {
			move2running.addParent(j);
		}
		Job oxoG = this.doOxoG(Arrays.asList(move2running));

		// indicate job is in uploading stage.
		Job move2uploading = gitMove(oxoG, "running-jobs", "uploading-jobs");
		Job uploadResults = doUpload(move2uploading);

		// indicate job is complete.
		Job move2finished = gitMove(uploadResults, "uploading-jobs", "completed-jobs");
	}
}
