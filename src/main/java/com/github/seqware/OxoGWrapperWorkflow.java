package com.github.seqware;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

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
	private String JSONrepoName = "oxog-job-control";
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

	private void init() {
		try {
			if (hasPropertyAndNotNull("OxoQScore")) {
				this.oxoQScore = getProperty("OxoQScore");
			}
			this.JSONrepo = getProperty("JSONrepo");
			this.JSONfolderName = getProperty("JSONfolderName");
			if (hasPropertyAndNotNull("JSONfileName")) {
				this.JSONfileName = getProperty("JSONfileName");
			}
			this.GITemail = getProperty("GITemail");
			this.GITname = getProperty("GITname");

			// if (hasPropertyAndNotNull("donorID")) {
			// this.donorID = getProperty("donorID");
			// }
			if (hasPropertyAndNotNull("bamNormalObjectID")) {
				this.bamNormalObjectID = getProperty("bamNormalObjectID");
			}
			if (hasPropertyAndNotNull("bamTumourObjectID")) {
				this.bamTumourObjectID = getProperty("bamTumourObjectID");
			}
			if (hasPropertyAndNotNull("sangerVCFObjectID")) {
				this.sangerVCFObjectID = getProperty("sangerVCFObjectID");
			}
			if (hasPropertyAndNotNull("dkfzemblVCFObjectID")) {
				this.dkfzemblVCFObjectID = getProperty("dkfzemblVCFObjectID");
			}
			if (hasPropertyAndNotNull("broadVCFObjectID")) {
				this.broadVCFObjectID = getProperty("broadVCFObjectID");
			}
			if (hasPropertyAndNotNull("uploadURL")) {
				this.uploadURL = getProperty("uploadURL");
			}
			
			Map<String,String> inputsFromJSON = JSONUtils.processJSONFile(this.JSONfileName);
			this.bamNormalObjectID = inputsFromJSON.get(JSONUtils.BAM_NORMAL_OBJECT_ID);
			this.bamTumourObjectID = inputsFromJSON.get(JSONUtils.BAM_TUMOUR_OBJECT_ID);
			this.broadVCFObjectID = inputsFromJSON.get(JSONUtils.BROAD_VCF_OBJECT_ID);
			this.sangerVCFObjectID = inputsFromJSON.get(JSONUtils.SANGER_VCF_OBJECT_ID);
			this.dkfzemblVCFObjectID = inputsFromJSON.get(JSONUtils.DKFZEMBL_VCF_OBJECT_ID);
			this.oxoQScore = inputsFromJSON.get(JSONUtils.OXOQ_SCORE);
			this.aliquotID = inputsFromJSON.get(JSONUtils.ALIQUOT_ID);
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private Job copyCollabTokenFile() {
		Job copyCollabTokenFileJob = this.getWorkflow().createBashJob("copy collab token file");
		copyCollabTokenFileJob.setCommand(
				"cp ~/.gnos/collab.token /home/seqware/downloads/icgc-storage-client-*/conf/application.properties");
		return copyCollabTokenFileJob;
	}

	private Job getBAMs(Job parentJob) {
		Job getNormalBamFileJob = this.getWorkflow().createBashJob("get Normal BAM file");
		getNormalBamFileJob.addParent(parentJob);
//		getNormalBamFileJob.setCommand(
//				"icgc-storage-client download --object-id " + this.bamNormalObjectID + " --output-dir /datastore/bam/normal/");

		// TODO: finish modifying this workflow to work without everything being built into a single seqware-derived image. This means
		// not assuming that icgc-client and the workflow itself will reside inside the same container as seqware. So the download commands
		// need to be re-written.
		String storageClientDockerCmdNormal = "docker run -e STORAGE_PROFILE=collab -v /datastore/bam/normal/logs/client.log:/icgc/icgc-storage-client/logs/client.log:rw -v /home/ubuntu/.gnos/collab.token:/icgc/icgc-storage-client/conf/application.properties:ro -v /datastore/bam/normal:/downloads/:rw icgc/icgc-storage-client /icgc/icgc-storage-client/bin/icgc-storage-client --object-id "
				+this.bamNormalObjectID+" --output-dir /downloads/";
		getNormalBamFileJob.setCommand(storageClientDockerCmdNormal);
		this.normalBAM = "/datastore/bam/normal/*.bam";
		
		
		Job getTumourBamFileJob = this.getWorkflow().createBashJob("get Tumour BAM file");
		getTumourBamFileJob.addParent(getNormalBamFileJob);
		//getTumourBamFileJob.setCommand(
		//		"icgc-storage-client download --object-id " + this.bamTumourObjectID + " --output-dir /datastore/bam/tumour/");
		String storageClientDockerCmdTumour = "docker run -e STORAGE_PROFILE=collab -v /datastore/bam/tumour/logs/client.log:/icgc/icgc-storage-client/logs/client.log:rw -v /home/ubuntu/.gnos/collab.token:/icgc/icgc-storage-client/conf/application.properties:ro -v /datastore/bam/tumour:/downloads/:rw icgc/icgc-storage-client /icgc/icgc-storage-client/bin/icgc-storage-client --object-id "
				+this.bamTumourObjectID+" --output-dir /downloads/";
		getTumourBamFileJob.setCommand(storageClientDockerCmdTumour);
		this.tumourBAM = "/datastore/bam/tumour/*.bam";
		
		return getTumourBamFileJob;
	}

	private Job getVCF(Job parentJob, String workflowName, String objectID) {
		Job getVCFJob = this.getWorkflow().createBashJob("get VCF for workflow " + workflowName);
		String outDir = "/datastore/vcf";
		getVCFJob.setCommand("icgc-storage-client download --object-id " + objectID + " --output-dir " + outDir);
		getVCFJob.addParent(parentJob);

		if (workflowName.equals("Sanger"))
			this.sangerVCF = outDir + "/somefile.vcf";
		else if (workflowName.equals("DKFZ_EMBL"))
			this.dkfzEmblVCF = outDir + "/somefile.vcf";
		else if (workflowName.equals("Broad"))
			this.broadVCF = outDir + "/somefile.vcf";

		return getVCFJob;
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
	
	private Job pullRepo(Job getReferenceDataJob) {
		Job installerJob = this.getWorkflow().createBashJob("install_dependencies");
		installerJob.getCommand().addArgument("if [[ ! -d ~/.ssh/ ]]; then  mkdir ~/.ssh; fi \n");
		installerJob.getCommand().addArgument("cp " + this.GITPemFile + " ~/.ssh/id_rsa \n");
		installerJob.getCommand().addArgument("chmod 600 ~/.ssh/id_rsa \n");
		installerJob.getCommand().addArgument("echo 'StrictHostKeyChecking no' > ~/.ssh/config \n");
		installerJob.getCommand().addArgument("if [[ -d " + this.JSONlocation + " ]]; then  exit 0; fi \n");
		installerJob.getCommand().addArgument("mkdir -p " + this.JSONlocation + " \n");
		installerJob.getCommand().addArgument("cd " + this.JSONlocation + " \n");
		installerJob.getCommand().addArgument("git config --global user.name " + this.GITname + " \n");
		installerJob.getCommand().addArgument("git config --global user.email " + this.GITemail + " \n");
		installerJob.getCommand().addArgument("git clone " + this.JSONrepo + " \n");
		installerJob.addParent(getReferenceDataJob);
		return (installerJob);
	}

	private Job gitMove(Job lastJob, String src, String dst) {
		Job manageGit = this.getWorkflow().createBashJob("git_manage_" + src + "_" + dst);
		String path = this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName;
		// String gitroot = this.JSONlocation + "/" + this.JSONrepoName;
		manageGit.getCommand().addArgument("git config --global user.name " + this.GITname + " \n");
		manageGit.getCommand().addArgument("git config --global user.email " + this.GITemail + " \n");
		manageGit.getCommand().addArgument("if [[ ! -d " + path + " ]]; then mkdir -p " + path + "; fi \n");
		manageGit.getCommand().addArgument("cd " + path + " \n");
		manageGit.getCommand().addArgument("# This is not idempotent: git pull \n");
		manageGit.getCommand().addArgument("git checkout master \n");
		manageGit.getCommand().addArgument("git reset --hard origin/master \n");
		manageGit.getCommand().addArgument("git fetch --all \n");
		manageGit.getCommand().addArgument("if [[ ! -d " + dst + " ]]; then mkdir " + dst + "; fi \n");
		manageGit.getCommand().addArgument("if [[ -d " + src + " ]]; then git mv " + path + "/" + src + "/"
				+ this.JSONfileName + " " + path + "/" + dst + "; fi \n");
		manageGit.getCommand().addArgument("git stage . \n");
		manageGit.getCommand().addArgument("git commit -m '" + dst + ": " + this.JSONfileName + "' \n");
		manageGit.getCommand().addArgument("git push \n");
		manageGit.addParent(lastJob);
		return (manageGit);
	}

	@Override
	public void buildWorkflow() {
		this.init();
		Job copyCollabToken = this.copyCollabTokenFile();

		// Pull the repo.
		Job pullRepo = this.pullRepo(copyCollabToken);
		// indicate job is in downloading stage.
		Job move2download = gitMove(pullRepo, "queued-jobs", "downloading-jobs");

		Job bamJob = this.getBAMs(move2download);
		Job sangerVCFJob = this.getVCF(move2download, "Sanger", this.sangerVCFObjectID);
		Job dkfzEmblVCFJob = this.getVCF(move2download, "DKFZ_EMBL", this.dkfzemblVCFObjectID);
		Job broadVCFJob = this.getVCF(move2download, "Broad", this.broadVCFObjectID);
		// indicate job is running.
		Job move2running = gitMove(null, "queued-jobs", "running-jobs");
		for (Job j : Arrays.asList(bamJob, sangerVCFJob, dkfzEmblVCFJob, broadVCFJob)) {
			move2running.addParent(j);
		}
		Job oxoG = this.doOxoG(Arrays.asList(move2running));

		// indicate job is in uploading stage.
		Job move2uploading = gitMove(oxoG, "running-jobs", "uploading-jobs");
		Job uploadResults = doUpload(move2uploading);

		// Job uploadMergeVCF = this.getWorkflow().createBashJob("upload merge
		// VCFs");
		// uploadMergeVCF.setCommand("rsync " + this.donorID + ".merge.vcf " +
		// this.uploadURL);
		// uploadMergeVCF.addParent(move2uploading);

		// indicate job is complete.
		Job move2finished = gitMove(null, "uploading-jobs", "completed-jobs");
		move2finished.addParent(uploadResults);
		// move2finished.addParent(uploadMergeVCF);
	}
}
