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

	private Job copyCredentials(Job parentJob){
		Job copy = this.getWorkflow().createBashJob("copy ~/.gnos");
		copy.setCommand("sudo cp -r ~/.gnos /datastore/credentials && ls -l /datastore/credentials");
		copy.addParent(parentJob);
		return copy;
	}
	
	enum BAMType{
		normal,tumour
	}
	private Job getBAM(Job parentJob, String objectID, BAMType bamType) {
		Job getBamFileJob = this.getWorkflow().createBashJob("get "+bamType.toString()+" BAM file");
		getBamFileJob.addParent(parentJob);
		String storageClientDockerCmdNormal ="sudo docker run --rm"
				+ " -e STORAGE_PROFILE=collab "
			    + " -v /datastore/bam/"+bamType.toString()+"/logs/:/icgc/icgc-storage-client/logs/:rw "
				+ " -v /datastore/credentials/collab.token:/icgc/icgc-storage-client/conf/application.properties:ro "
			    + " -v /datastore/bam/"+bamType.toString()+"/:/tmp/:rw"
			    + " icgc/icgc-storage-client "
				+ " /icgc/icgc-storage-client/bin/icgc-storage-client download --object-id "
					+ objectID +" --output-dir /tmp/";
		getBamFileJob.setCommand(storageClientDockerCmdNormal);

		return getBamFileJob;
	}

	enum Workflow {
		sanger, dkfz_embl, broad
	}
	// This will download VCFs for a workflow, based on an object ID. It will also perform the VCF Primitives
	// operation on the indel VCF and then do VCFCombine on
	// the indel, sv, and snv VCFs.
	private Job getVCF(Job parentJob, Workflow workflowName, String objectID) {
		Job getVCFJob = this.getWorkflow().createBashJob("get VCF for workflow " + workflowName);
		String outDir = "/datastore/vcf/"+workflowName;
		String getVCFCommand = "sudo docker run --rm"
				+ " -e STORAGE_PROFILE=collab "
			    + " -v "+outDir+"/logs/:/icgc/icgc-storage-client/logs/:rw "
				+ " -v /datastore/credentials/collab.token:/icgc/icgc-storage-client/conf/application.properties:ro "
			    + " -v "+outDir+"/:/tmp/:rw"
	    		+ " icgc/icgc-storage-client "
				+ " /icgc/icgc-storage-client/bin/icgc-storage-client download --object-id " + objectID+" --output-dir /tmp/";
		getVCFJob.setCommand(getVCFCommand);
		getVCFJob.addParent(parentJob);

		// TODO: Many of these steps below could probably be combined into a single Job
		// that makes runs a single docker container, but executes multiple commands.		
		Job bcfToolsNormJob = this.getWorkflow().createBashJob("run VCF primitives on indel");
		String runBCFToolsNormCommand = "sudo docker run --rm "
					+ " -v "+outDir+"/*.somatic.indel.vcf.gz:/datastore/datafile.vcf.gz "
					+ " -v /datastore/refdata/public:/ref"
					+ " compbio/ngseasy-base:a1.0-002 " 
					+ " bcftools norm -c w -m -any -O -z -f /ref/Homo_sapiens_assembly19.fasta  /datastore/datafile.vcf.gz "  
				+ " > "+outDir+"/somatic.indel.bcftools-norm.vcf.gz";
		bcfToolsNormJob.setCommand(runBCFToolsNormCommand);
		bcfToolsNormJob.addParent(getVCFJob);
		
		Job unzip = this.getWorkflow().createBashJob("unzip VCFs");
		unzip.setCommand("gunzip "+outDir+"/*.vcf.gz");
		unzip.addParent(bcfToolsNormJob);

		//vcfcombine requries VCF files be unzipped.
		Job vcfCombineJob = this.getWorkflow().createBashJob("run VCF Combine on VCFs for workflow");
		String runVCFCombineCommand = "sudo docker run --rm "
					+ " -v "+outDir+"/:/VCFs/"
					+ " compbio/ngseasy-base:a1.0-002 vcfcombine /VCFs/*somatic.snv_mnv.vcf /VCFs/*somatic.indel.PRIMITIVES.vcf /VCFs/*somatic.sv.vcf" 
				+ " > "+outDir+"/snv_AND_indel_AND_sv.vcf";
		vcfCombineJob.setCommand(runVCFCombineCommand);
		vcfCombineJob.addParent(unzip);
		
		//OxoG requires inputs be in BGZIP format.
		Job bgZip = this.getWorkflow().createBashJob("bgzip combined vcf");
		bgZip.setCommand("bgzip -f "+outDir+"/snv_AND_indel_AND_sv.vcf");
		bgZip.addParent(vcfCombineJob);
		
		
		if (workflowName.equals("Sanger"))
			this.sangerVCF = outDir + "/snv_AND_indel_AND_sv.vcf.gz";
		else if (workflowName.equals("DKFZ_EMBL"))
			this.dkfzEmblVCF = outDir + "/snv_AND_indel_AND_sv.vcf.gz";
		else if (workflowName.equals("Broad"))
			this.broadVCF = outDir + "/snv_AND_indel_AND_sv.vcf.gz";

		return bgZip;
	}

	enum VCFType{
		sv, snv, indel
	}
	private Job combineVCFsByType(VCFType vcfType, Job ... parents)
	{
		Job vcfCombineJob = this.getWorkflow().createBashJob("vcfcombine for "+vcfType);
		
		String sharedDir = "/datastore/vcf/";
		String runVCFCombineCommand = "sudo docker run --rm "
				+ " -v "+sharedDir+"/:/VCFs/"
				+ " compbio/ngseasy-base:a1.0-002 vcfcombine ";
		
		for (Workflow w : Workflow.values())
		{
			//this is naive - the file names will probably be more complicated than this. Need to get a complete download to see exactlty...
			runVCFCombineCommand += " /VCFs/"+w+"/"+vcfType+".vcf ";
		}
		
		runVCFCombineCommand += " > "+sharedDir+"/combined_"+vcfType+".vcf";
		
		vcfCombineJob.setCommand(runVCFCombineCommand);
		
		return vcfCombineJob;
	}
	
	private Job doOxoG(Job parent) {
		Job runOxoGWorkflow = this.getWorkflow().createBashJob("Run OxoG Filter");
		String oxogMounts = " -v /datastore/refdata/:/cga/fh/pcawg_pipeline/refdata/ "
				+ " -v /datastore/oncotator_db/:/cga/fh/pcawg_pipeline/refdata/public/oncotator_db/ "  
				+ " -v /datastore/oxog_workspace/:/cga/fh/pcawg_pipeline/jobResults_pipette/jobs/"+this.aliquotID+"/:rw " 
				+ " -v /datastore/bam/:/datafiles/BAM/  -v /datastore/vcf/:/datafiles/VCF/ "
				+ " -v /datastore/oxog_results/:/cga/fh/pcawg_pipeline/jobResults_pipette/results:rw ";
		String oxogCommand = "/cga/fh/pcawg_pipeline/pipelines/run_one_pipeline.bash pcawg /cga/fh/pcawg_pipeline/pipelines/oxog_pipeline.py "
				+ this.aliquotID + " " + this.tumourBAM + " " + this.normalBAM + " " + this.oxoQScore + " "
				+ this.sangerVCF + " " + this.dkfzEmblVCF + " " + this.broadVCF;
		runOxoGWorkflow.setCommand(
				"sudo docker run --name=\"oxog_filter\" "+oxogMounts+" oxog " + oxogCommand);
		
		runOxoGWorkflow.addParent(parent);
		
		Job getLogs = this.getOxoGLogs(runOxoGWorkflow);

		return getLogs;
	}

	private Job doVariantBam(Job parent) {
		Job runOxoGWorkflow = this.getWorkflow().createBashJob("Run variantbam");
		String oxogMounts = " -v /datastore/refdata/:/cga/fh/pcawg_pipeline/refdata/ "
				+ " -v /datastore/oncotator_db/:/cga/fh/pcawg_pipeline/refdata/public/oncotator_db/ "  
				+ " -v /datastore/variantbam_workspace/:/cga/fh/pcawg_pipeline/jobResults_pipette/jobs/"+this.aliquotID+"/:rw " 
				+ " -v /datastore/bam/:/datafiles/BAM/  -v /datastore/vcf/:/datafiles/VCF/ "
				+ " -v /datastore/variantbam_results/:/cga/fh/pcawg_pipeline/jobResults_pipette/results:rw ";
		// TODO: Update this to use the per-VCF-type combined VCFs instead of the per-workflow combined VCFs.
		String oxogCommand = "/cga/fh/pcawg_pipeline/pipelines/run_one_pipeline.bash pcawg /cga/fh/pcawg_pipeline/pipelines/variantbam_pipeline.py "
				+ this.aliquotID + " " + this.tumourBAM + " " + this.normalBAM + " " + this.oxoQScore + " "
				+ this.sangerVCF + " " + this.dkfzEmblVCF + " " + this.broadVCF;
		runOxoGWorkflow.setCommand(
				"sudo docker run --name=\"variantbam\" "+oxogMounts+" oxog " + oxogCommand);
		
		runOxoGWorkflow.addParent(parent);
		
		Job getLogs = this.getOxoGLogs(runOxoGWorkflow);

		return getLogs;
	}
	
	private Job getOxoGLogs(Job parent) {
		Job getLog = this.getWorkflow().createBashJob("get OxoG docker logs");
		// This will get the docker logs and print them to stdout, but we may also want to get the logs
		// in the mounted oxog_workspace dir...
		getLog.setCommand("sudo docker logs oxog_run");
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
		
		Job copy = this.copyCredentials(pullRepo);
		
		// indicate job is in downloading stage.
		Job move2download = gitMove(copy, "queued-jobs", "downloading-jobs");
		// These jobs will all reun parallel. The BAM jobs just download, but the VCF jobs also do some
		// processing (bcftools norm and vcfcombine) on the downloaded files.
		Job normalBamJob = this.getBAM(move2download,this.bamNormalObjectID,BAMType.normal);
		this.normalBAM = "/datastore/bam/normal/*.bam";
		Job tumourBamJob = this.getBAM(move2download,this.bamTumourObjectID,BAMType.tumour);
		this.tumourBAM = "/datastore/bam/tumour/*.bam";
		Job sangerVCFJob = this.getVCF(move2download, Workflow.sanger, this.sangerVCFObjectID);
		Job dkfzEmblVCFJob = this.getVCF(move2download, Workflow.dkfz_embl, this.dkfzemblVCFObjectID);
		Job broadVCFJob = this.getVCF(move2download, Workflow.broad, this.broadVCFObjectID);
		
		// After we've processed all VCFs on a per-workflow basis, we also need to do a vcfcombine 
		// on the *types* of VCFs, for the minibam generator. The per-workflow combined VCFs will
		// be used by the OxoG filter.
		Job combineAllSVs = this.combineVCFsByType(VCFType.sv, sangerVCFJob, dkfzEmblVCFJob, broadVCFJob, normalBamJob, tumourBamJob);
		Job combineAllSNVs = this.combineVCFsByType(VCFType.snv, sangerVCFJob, dkfzEmblVCFJob, broadVCFJob, normalBamJob, tumourBamJob);
		Job combineAllIndels = this.combineVCFsByType(VCFType.indel, sangerVCFJob, dkfzEmblVCFJob, broadVCFJob, normalBamJob, tumourBamJob);
		
		// indicate job is running.
		//Note: because of the way that addParent works, we need to pass at least ONE parent
		//object here, and then add the rest in a loop below.
		Job move2running = gitMove(combineAllSVs, "queued-jobs", "running-jobs");
		for (Job j : Arrays.asList(combineAllSNVs, combineAllIndels)) {
			move2running.addParent(j);
		}
		// OxoG will run after move2running. Move2running will run after all the jobs that perform input file downloads have finished.  
		Job oxoG = this.doOxoG(move2running);
		// variantbam will run parallel to oxog
		Job variantBam = this.doVariantBam(move2running);

		// indicate job is in uploading stage.
		Job move2uploading = gitMove(oxoG, "running-jobs", "uploading-jobs");
		// make sure that we don't move to uploading state until after both OxoG and variantbam are finished.
		move2uploading.addParent(variantBam);
		Job uploadResults = doUpload(move2uploading);

		// indicate job is complete.
		Job move2finished = gitMove(uploadResults, "uploading-jobs", "completed-jobs");
	}
}
