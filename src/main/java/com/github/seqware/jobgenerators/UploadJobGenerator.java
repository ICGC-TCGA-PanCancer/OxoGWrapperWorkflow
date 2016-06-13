package com.github.seqware.jobgenerators;

import java.util.Arrays;
import java.util.List;

import com.github.seqware.GitUtils;
import com.github.seqware.TemplateUtils;

import net.sourceforge.seqware.pipeline.workflowV2.AbstractWorkflowDataModel;
import net.sourceforge.seqware.pipeline.workflowV2.model.Job;

public class UploadJobGenerator extends JobGeneratorBase {

	private String gnosMetadataUploadURL;
	private String uploadURL;
	private String uploadKey;
	private String donorID;
	private String specimenID;
	private String snvPadding;
	private String indelPadding;
	private String svPadding;
	private String workflowURL;
	private String workflowSourceURL;
	private String changelogURL;
	private String gnosKey;
	private String normalMetdataURL;
	private String tumourMetadataURLs;
	private String studyRefNameOverride;
	private String oxoQScore;

	/**
	 * Uploads files. Will use the vcf-upload script in pancancer/pancancer_upload_download:1.7 to generate metadata.xml, analysis.xml, and the GTO file, and
	 * then rsync everything to a staging server. 
	 * @param parentJob
	 * @return
	 */
	public Job doUpload(AbstractWorkflowDataModel workflow, Job parentJob, List<String> bamFiles, List<String> vcfs) {
		String moveToFailed = GitUtils.gitMoveCommand("uploading-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, workflow.getWorkflowBaseDir() + "/scripts/");
		// Will need to run gtupload to generate the analysis.xml and manifest files (but not actually upload). 
		Job generateAnalysisFilesVCFs = generateVCFMetadata(workflow, parentJob, moveToFailed, vcfs);
		
		Job generateAnalysisFilesBAMs = generateBAMMetadata(workflow, parentJob, moveToFailed, bamFiles);
	
		String gnosServer = this.gnosMetadataUploadURL.replace("http://", "").replace("https://", "").replace("/", "");
		//Note: It was decided there should be two uploads: one for minibams and one for VCFs (for people who want VCFs but not minibams).
		Job uploadVCFResults = workflow.getWorkflow().createBashJob("upload VCF results");
		String uploadVCFCommand = "sudo chmod 0600 /datastore/credentials/rsync.key\n"
								+ "UPLOAD_PATH=$( echo \""+this.uploadURL+"\" | sed 's/\\(.*\\)\\:\\(.*\\)/\\2/g' )\n"
								+ "VCF_UUID=$(grep server_path /datastore/files_for_upload/manifest.xml  | sed 's/.*server_path=\\\"\\(.*\\)\\\" .*/\\1/g')\n"
								+ "( rsync -avtuz -e 'ssh -o UserKnownHostsFile=/datastore/credentials/known_hosts -o IdentitiesOnly=yes -o BatchMode=yes -o PasswordAuthentication=no -o PreferredAuthentications=publickey -i "+this.uploadKey+"'"
										+ " --rsync-path=\"mkdir -p $UPLOAD_PATH/"+gnosServer+"/$VCF_UUID && rsync\" /datastore/files_for_upload/ " + this.uploadURL+ "/"+gnosServer + "/$VCF_UUID ) ";
		uploadVCFCommand += (" || " + moveToFailed);
		uploadVCFResults.setCommand(uploadVCFCommand);
		uploadVCFResults.addParent(generateAnalysisFilesVCFs);
		
		Job uploadBAMResults = workflow.getWorkflow().createBashJob("upload BAM results");
		String uploadBAMcommand = "sudo chmod 0600 /datastore/credentials/rsync.key\n"
								+ "UPLOAD_PATH=$( echo \""+this.uploadURL+"\" | sed 's/\\(.*\\)\\:\\(.*\\)/\\2/g' )\n"
								+ "BAM_UUID=$(grep server_path /datastore/variantbam_results/manifest.xml  | sed 's/.*server_path=\\\"\\(.*\\)\\\" .*/\\1/g')\n"
								+ "( rsync -avtuz -e 'ssh -o UserKnownHostsFile=/datastore/credentials/known_hosts -o IdentitiesOnly=yes -o BatchMode=yes -o PasswordAuthentication=no -o PreferredAuthentications=publickey -i "+this.uploadKey+"'"
										+ " --rsync-path=\"mkdir -p $UPLOAD_PATH/"+gnosServer+"/$BAM_UUID && rsync\" /datastore/variantbam_results/ " + this.uploadURL+ "/"+gnosServer + "/$BAM_UUID ) ";
		uploadBAMcommand += (" || " + moveToFailed);
		uploadBAMResults.setCommand(uploadBAMcommand);
		uploadBAMResults.addParent(generateAnalysisFilesBAMs);
		//uploadBAMResults.addParent(uploadVCFResults);
		return uploadBAMResults;
	}

	public Job generateBAMMetadata(AbstractWorkflowDataModel workflow, Job parentJob, String moveToFailed, List<String> files) {
		Job generateAnalysisFilesBAMs = workflow.getWorkflow().createBashJob("generate_analysis_files_for_BAM_upload");
		
		String bams = "";
		String bamIndicies = "";
		String bamMD5Sums = "";
		String bamIndexMD5Sums = "";
		String generateAnalysisFilesBAMsCommand = "";
		generateAnalysisFilesBAMsCommand += "sudo chmod a+rw -R /datastore/variantbam_results/ &&\n";
		//I don't think distinct() should be necessary.
		//for (String file : this.filesForUpload.stream().filter( p -> p.contains(".bam") || p.contains(".bai") ).distinct().collect(Collectors.toList()) )
		for (String file : files )
		{
			file = file.trim();
			//md5sum test_files/tumour_minibam.bam.bai | cut -d ' ' -f 1 > test_files/tumour_minibam.bai.md5
			generateAnalysisFilesBAMsCommand += " md5sum "+file+" | cut -d ' ' -f 1 > "+file+".md5 ; \n";
			
			if (file.contains(".bai") )
			{
				bamIndicies += file + ",";
				bamIndexMD5Sums += file + ".md5" + ",";
			}
			else
			{
				bams += file + ",";
				bamMD5Sums += file + ".md5" + ",";
			}
		}
		String descriptionEnd = TemplateUtils.getRenderedTemplate("analysisDescriptionSuffix.template");
		String bamDescription = TemplateUtils.getRenderedTemplate(Arrays.stream(new String [][] {
				{"donorID", this.donorID}, {"specimenID", this.specimenID}, {"snvPadding", String.valueOf(this.snvPadding)},
				{"indelPadding", String.valueOf(this.indelPadding)}, {"svPadding", String.valueOf(this.svPadding)},
				{"workflowName", workflow.getName()}, {"workflowVersion", workflow.getVersion()}, {"workflowURL", this.workflowURL},
				{"workflowSrcURL", this.workflowSourceURL}, {"changeLogURL", this.changelogURL}, {"descriptionSuffix", descriptionEnd}
			}).collect(this.collectToMap), "analysisBAMDescription.template");
		generateAnalysisFilesBAMsCommand += TemplateUtils.getRenderedTemplate(Arrays.stream(new String[][] {
				{ "gnosKey", this.gnosKey }, { "gnosMetadataUploadURL", this.gnosMetadataUploadURL }, { "bamDescription", bamDescription },
				///{ "normalMetadataURL", this.normalMetdataURL } , { "tumourMetadataURLs", String.join("," , this.tumours.stream().map(t -> t.getTumourMetdataURL()).collect(Collectors.toList()) ) },
				{ "normalMetadataURL", this.normalMetdataURL } , { "tumourMetadataURLs", this.tumourMetadataURLs },
				{ "bams", bams }, { "bamIndicies", bamIndicies}, { "bamMD5Sums", bamMD5Sums }, { "bamIndexMD5Sums", bamIndexMD5Sums}, 
				{ "studyRefNameOverride", this.studyRefNameOverride }, { "workflowVersion", workflow.getVersion() } 
			}).collect(this.collectToMap),"generateBAMAnalysisMetadata.template");

		generateAnalysisFilesBAMs.setCommand("( "+generateAnalysisFilesBAMsCommand+" ) || "+moveToFailed);
		generateAnalysisFilesBAMs.addParent(parentJob);
		return generateAnalysisFilesBAMs;
	}

	public Job generateVCFMetadata(AbstractWorkflowDataModel workflow , Job parentJob, String moveToFailed, List<String> files) {
		Job generateAnalysisFilesVCFs = workflow.getWorkflow().createBashJob("generate_analysis_files_for_VCF_upload");
		//Files need to be copied to the staging directory
		String vcfs = "";
		String vcfIndicies = "";
		String vcfMD5Sums = "";
		String vcfIndexMD5Sums = "";
		
		String tars = "";
		String tarMD5Sums = "";
		
		String generateAnalysisFilesVCFCommand = "";
		//I don't think "distinct" should be necessary here, but there were weird duplicates popping up in the list.
		//for (String file : this.filesForUpload.stream().filter(p -> ((p.contains(".vcf") || p.endsWith(".tar")) && !( p.contains("SNVs_from_INDELs") || p.contains("extracted-snv"))) ).distinct().collect(Collectors.toList()) )
		for (String file : files )
		{
			file = file.trim();
			//md5sum test_files/tumour_minibam.bam.bai | cut -d ' ' -f 1 > test_files/tumour_minibam.bai.md5
			generateAnalysisFilesVCFCommand += "md5sum "+file+" | cut -d ' ' -f 1 > "+file+".md5 ; \n";
			
			if (file.endsWith(".tar"))
			{
				tars += file + ",";
				tarMD5Sums += file+".md5,";
			}
			else if (file.contains(".tbi") || file.contains(".idx"))
			{
				vcfIndicies += file + ",";
				vcfIndexMD5Sums += file + ".md5," ;
			}
			else
			{
				vcfs += file + ",";
				vcfMD5Sums += file + ".md5,";	 
			}
		}
		// trim trailing commas so that you don't get ",," in the "--vcfs ..." arg to gt-download-upload-wrapper
		// since that will result in empty files in the analysis and things will break.
		vcfs = vcfs.substring(0,vcfs.length()-1);
		vcfMD5Sums = vcfMD5Sums.substring(0,vcfMD5Sums.length()-1);
		vcfIndicies = vcfIndicies.substring(0,vcfIndicies.length()-1);
		vcfIndexMD5Sums = vcfIndexMD5Sums.substring(0,vcfIndexMD5Sums.length()-1);
		
		String descriptionEnd = TemplateUtils.getRenderedTemplate("analysisDescriptionSuffix.template");
		String vcfDescription = TemplateUtils.getRenderedTemplate(Arrays.stream(new String[][]{
				{ "OxoQScore",this.oxoQScore }, { "donorID",this.donorID }, { "specimenID",this.specimenID },
				{ "workflowName",workflow.getName() }, { "workflowVersion",workflow.getVersion() }, { "workflowURL",this.workflowURL },
				{ "workflowSrcURL",this.workflowSourceURL }, { "changeLogURL",this.changelogURL }, { "descriptionSuffix",descriptionEnd },
			}).collect(this.collectToMap), "analysisVCFDescription.template");

		
		generateAnalysisFilesVCFCommand += TemplateUtils.getRenderedTemplate(Arrays.stream(new String[][] {
				{ "gnosKey", this.gnosKey }, { "gnosMetadataUploadURL", this.gnosMetadataUploadURL }, { "vcfDescription", vcfDescription },
				{ "normalMetadataURL", this.normalMetdataURL } , { "tumourMetadataURLs", this.tumourMetadataURLs },
				{ "vcfs", vcfs }, { "tars", tars }, { "tarMD5Sums", tarMD5Sums }, { "vcfIndicies", vcfIndicies}, { "vcfMD5Sums", vcfMD5Sums },
				{ "vcfIndexMD5Sums", vcfIndexMD5Sums}, { "studyRefNameOverride", this.studyRefNameOverride }, { "workflowVersion", workflow.getVersion() } 
			}).collect(this.collectToMap),"generateVCFAnalysisMetadata.template");
		generateAnalysisFilesVCFs.setCommand("( "+generateAnalysisFilesVCFCommand+ " ) || "+moveToFailed);
		generateAnalysisFilesVCFs.addParent(parentJob);
		return generateAnalysisFilesVCFs;
	}

	public String getGnosMetadataUploadURL() {
		return this.gnosMetadataUploadURL;
	}

	public void setGnosMetadataUploadURL(String gnosMetadataUploadURL) {
		this.gnosMetadataUploadURL = gnosMetadataUploadURL;
	}

	public String getUploadURL() {
		return this.uploadURL;
	}

	public void setUploadURL(String uploadURL) {
		this.uploadURL = uploadURL;
	}

	public String getUploadKey() {
		return this.uploadKey;
	}

	public void setUploadKey(String uploadKey) {
		this.uploadKey = uploadKey;
	}

	public String getDonorID() {
		return this.donorID;
	}

	public void setDonorID(String donorID) {
		this.donorID = donorID;
	}

	public String getSpecimenID() {
		return this.specimenID;
	}

	public void setSpecimenID(String specimenID) {
		this.specimenID = specimenID;
	}

	public String getSnvPadding() {
		return this.snvPadding;
	}

	public void setSnvPadding(String snvPadding) {
		this.snvPadding = snvPadding;
	}

	public String getIndelPadding() {
		return this.indelPadding;
	}

	public void setIndelPadding(String indelPadding) {
		this.indelPadding = indelPadding;
	}

	public String getSvPadding() {
		return this.svPadding;
	}

	public void setSvPadding(String svPadding) {
		this.svPadding = svPadding;
	}

	public String getWorkflowURL() {
		return this.workflowURL;
	}

	public void setWorkflowURL(String workflowURL) {
		this.workflowURL = workflowURL;
	}

	public String getWorkflowSourceURL() {
		return this.workflowSourceURL;
	}

	public void setWorkflowSourceURL(String workflowSourceURL) {
		this.workflowSourceURL = workflowSourceURL;
	}

	public String getChangelogURL() {
		return this.changelogURL;
	}

	public void setChangelogURL(String changelogURL) {
		this.changelogURL = changelogURL;
	}

	public String getGnosKey() {
		return this.gnosKey;
	}

	public void setGnosKey(String gnosKey) {
		this.gnosKey = gnosKey;
	}

	public String getNormalMetdataURL() {
		return this.normalMetdataURL;
	}

	public void setNormalMetdataURL(String normalMetdataURL) {
		this.normalMetdataURL = normalMetdataURL;
	}

	public String getTumourMetadataURLs() {
		return this.tumourMetadataURLs;
	}

	public void setTumourMetadataURLs(String tumourMetadataURLs) {
		this.tumourMetadataURLs = tumourMetadataURLs;
	}

	public String getStudyRefNameOverride() {
		return this.studyRefNameOverride;
	}

	public void setStudyRefNameOverride(String studyRefNameOverride) {
		this.studyRefNameOverride = studyRefNameOverride;
	}

	public String getOxoQScore() {
		return this.oxoQScore;
	}

	public void setOxoQScore(String oxoQScore) {
		this.oxoQScore = oxoQScore;
	}
}
