package com.github.seqware;

import java.util.ArrayList;
import java.util.List;

import net.sourceforge.seqware.pipeline.workflowV2.AbstractWorkflowDataModel;

public abstract class BaseOxoGWrapperWorkflow extends AbstractWorkflowDataModel {
	//ugh... so many fields. There's probably a better way to do this, just no time right now.
	protected String oxoQScore = "";
	// protected String donorID;
	protected String aliquotID;
	protected String bamNormalObjectID;
	protected String bamNormalIndexObjectID;
	protected String bamTumourObjectID;
	protected String bamTumourIndexObjectID;
	
	protected String sangerSNVVCFObjectID;
	protected String dkfzemblSNVVCFObjectID;
	protected String broadSNVVCFObjectID;
	protected String museSNVVCFObjectID;

	protected String sangerIndelVCFObjectID;
	protected String dkfzemblIndelVCFObjectID;
	protected String broadIndelVCFObjectID;

	protected String sangerSVVCFObjectID;
	protected String dkfzemblSVVCFObjectID;
	protected String broadSVVCFObjectID;

	protected String sangerSNVIndexObjectID;
	protected String dkfzemblSNVIndexObjectID;
	protected String broadSNVIndexObjectID;
	protected String museSNVIndexObjectID;

	protected String sangerIndelIndexObjectID;
	protected String dkfzemblIndelIndexObjectID;
	protected String broadIndelIndexObjectID;

	protected String sangerSVIndexObjectID;
	protected String dkfzemblSVIndexObjectID;
	protected String broadSVIndexObjectID;
	
	protected String uploadURL;

	protected String JSONrepo = null;
	protected String JSONrepoName = "oxog-ops";
	protected String JSONfolderName = null;
	protected String JSONlocation = "/datastore/gitroot";
	protected String JSONfileName = null;

	protected String GITemail = "";
	protected String GITname = "ICGC AUTOMATION";
	protected String GITPemFile = "";

	//These will be needed so that vcf-uploader can generate the analysis.xml and manifest.xml files
	protected String tumourMetdataURL;
	protected String normalMetdataURL;
	
	protected String tumourBAMFileName;
	protected String normalBAMFileName;
	protected String sangerSNVName;
	protected String dkfzEmblSNVName;
	protected String broadSNVName;
	protected String museSNVName;

	protected String sangerSVName;
	protected String dkfzEmblSVName;
	protected String broadSVName;

	protected String sangerIndelName;
	protected String dkfzEmblIndelName;
	protected String broadIndelName;
	
	protected int snvPadding = 10;
	protected int svPadding = 10;
	protected int indelPadding = 200;
	
	protected String storageSource = "collab";
	
	protected boolean gitMoveTestMode = false;
	
	protected List<String> filesToUpload = new ArrayList<String>();
	
	//Paths to VCFs generated by merging types across workflows. 
	protected String snvVCF;
	protected String svVCF;
	protected String indelVCF;
	protected String sangerNormalizedIndelVCFName;
	protected String broadNormalizedIndelVCFName;
	protected String dkfzEmblNormalizedIndelVCFName;
	//protected String museNormalizedIndelVCFName;

	protected String sangerExtractedSNVVCFName;
	protected String broadExtractedSNVVCFName;
	protected String dkfzEmblExtractedSNVVCFName;
	//protected String museExtractedSNVVCFName;

	protected String sangerGnosID;
	protected String broadGnosID;
	protected String dkfzemblGnosID;
	protected String museGnosID;
	
	//This could be used implement a sort of local-file mode. For now, it's just used to speed up testing.
	protected boolean skipDownload = false;
	protected boolean skipUpload = false;
	
	//Path to reference file usd for normalization, *relative* to /datastore/refdata
	protected String refFile = "pcawg/genome.fa";
	protected String tumourBamGnosID;
	protected String normalBamGnosID;
	protected String uploadKey;
	
	//These two variables can be used to skip running OxoG and variant bam, in case they have already been run.
	//Intended to speed up testing by skipping these steps when results already exist.
	protected boolean skipOxoG = false;
	protected boolean skipVariantBam = false;
	protected boolean skipAnnotation = false;
	
	protected String normalMinibamPath;
	protected String tumourMinibamPath;
	
	/**
	 * Get a property name that is mandatory
	 * @param propName The name of the property
	 * @return The property, as a String. Convert to other types if you need to.
	 * @throws Exception An Exception with the message "Property with key <i>propName</i> cannot be null" will be thrown if property is not found.
	 */
	private String getMandatoryProperty(String propName) throws Exception
	{
		if (hasPropertyAndNotNull(propName)) {
			return getProperty(propName);
		}
		else {
			throw new Exception ("Property with key "+propName+ " cannot be null!");
		}
	}

	/**
	 * Initial setup.
	 */
	protected void init() {
		try {
			
			this.oxoQScore = this.getMandatoryProperty(JSONUtils.OXOQ_SCORE);
			this.JSONrepo = this.getMandatoryProperty("JSONrepo");
			this.JSONrepoName = this.getMandatoryProperty("JSONrepoName");
			this.JSONfolderName = this.getMandatoryProperty("JSONfolderName");
			this.JSONfileName = this.getMandatoryProperty("JSONfileName");
			this.GITemail = this.getMandatoryProperty("GITemail");
			this.GITname = this.getMandatoryProperty("GITname");
			
			this.bamNormalObjectID = this.getMandatoryProperty(JSONUtils.BAM_NORMAL_OBJECT_ID);
			this.normalMetdataURL = this.getMandatoryProperty(JSONUtils.BAM_NORMAL_METADATA_URL);
			this.bamTumourObjectID = this.getMandatoryProperty(JSONUtils.BAM_TUMOUR_OBJECT_ID);
			this.tumourMetdataURL = this.getMandatoryProperty(JSONUtils.BAM_TUMOUR_METADATA_URL);
			this.sangerSNVVCFObjectID = this.getMandatoryProperty(JSONUtils.SANGER_SNV_VCF_OBJECT_ID);
			this.dkfzemblSNVVCFObjectID = this.getMandatoryProperty(JSONUtils.DKFZEMBL_SNV_VCF_OBJECT_ID);
			this.broadSNVVCFObjectID = this.getMandatoryProperty(JSONUtils.BROAD_SNV_VCF_OBJECT_ID);
			this.museSNVVCFObjectID = this.getMandatoryProperty(JSONUtils.MUSE_VCF_OBJECT_ID);
			this.uploadURL = this.getMandatoryProperty("uploadURL");
			this.aliquotID = this.getMandatoryProperty(JSONUtils.ALIQUOT_ID);
			
			this.GITPemFile = this.getMandatoryProperty("GITPemFile");

			this.normalBAMFileName = this.getMandatoryProperty(JSONUtils.BAM_NORMAL_FILE_NAME);
			this.tumourBAMFileName = this.getMandatoryProperty(JSONUtils.BAM_TUMOUR_FILE_NAME);
			
			this.sangerSNVName = this.getMandatoryProperty(JSONUtils.SANGER_SNV_VCF_NAME);
			this.broadSNVName = this.getMandatoryProperty(JSONUtils.BROAD_SNV_VCF_NAME);
			this.dkfzEmblSNVName = this.getMandatoryProperty(JSONUtils.DKFZEMBL_SNV_VCF_NAME);
			this.museSNVName = this.getMandatoryProperty(JSONUtils.MUSE_VCF_NAME);
			
			this.sangerIndelName = this.getMandatoryProperty(JSONUtils.SANGER_INDEL_VCF_NAME);
			this.dkfzEmblIndelName = this.getMandatoryProperty(JSONUtils.DKFZEMBL_INDEL_VCF_NAME);
			this.broadIndelName = this.getMandatoryProperty(JSONUtils.BROAD_INDEL_VCF_NAME);

			this.sangerSVName = this.getMandatoryProperty(JSONUtils.SANGER_SV_VCF_NAME);
			this.dkfzEmblSVName = this.getMandatoryProperty(JSONUtils.DKFZEMBL_SV_VCF_NAME);
			this.broadSVName = this.getMandatoryProperty(JSONUtils.BROAD_SV_VCF_NAME);

			this.sangerSVVCFObjectID = this.getMandatoryProperty(JSONUtils.SANGER_SV_VCF_OBJECT_ID);
			this.broadSVVCFObjectID = this.getMandatoryProperty(JSONUtils.BROAD_SV_VCF_OBJECT_ID);
			this.dkfzemblSVVCFObjectID = this.getMandatoryProperty(JSONUtils.DKFZEMBL_SV_VCF_OBJECT_ID);
			
			this.sangerIndelVCFObjectID = this.getMandatoryProperty(JSONUtils.SANGER_INDEL_VCF_OBJECT_ID);
			this.broadIndelVCFObjectID = this.getMandatoryProperty(JSONUtils.BROAD_INDEL_VCF_OBJECT_ID);
			this.dkfzemblIndelVCFObjectID = this.getMandatoryProperty(JSONUtils.DKFZEMBL_INDEL_VCF_OBJECT_ID);
			
			this.sangerSNVIndexObjectID = this.getMandatoryProperty(JSONUtils.SANGER_SNV_INDEX_OBJECT_ID);
			this.broadSNVIndexObjectID = this.getMandatoryProperty(JSONUtils.BROAD_SNV_INDEX_OBJECT_ID);
			this.dkfzemblSNVIndexObjectID = this.getMandatoryProperty(JSONUtils.DKFZEMBL_SNV_INDEX_OBJECT_ID);
			this.museSNVIndexObjectID = this.getMandatoryProperty(JSONUtils.MUSE_SNV_INDEX_OBJECT_ID);
			
			this.sangerSVIndexObjectID = this.getMandatoryProperty(JSONUtils.SANGER_SNV_INDEX_OBJECT_ID);
			this.broadSVIndexObjectID = this.getMandatoryProperty(JSONUtils.BROAD_SV_INDEX_OBJECT_ID);
			this.dkfzemblSVIndexObjectID = this.getMandatoryProperty(JSONUtils.DKFZEMBL_SV_INDEX_OBJECT_ID);
			
			this.sangerIndelIndexObjectID = this.getMandatoryProperty(JSONUtils.SANGER_INDEL_INDEX_OBJECT_ID);
			this.broadIndelIndexObjectID = this.getMandatoryProperty(JSONUtils.BROAD_INDEL_INDEX_OBJECT_ID);
			this.dkfzemblIndelIndexObjectID = this.getMandatoryProperty(JSONUtils.DKFZEMBL_INDEL_INDEX_OBJECT_ID);
			
			this.bamNormalIndexObjectID = this.getMandatoryProperty(JSONUtils.BAM_NORMAL_INDEX_OBJECT_ID);
			this.bamTumourIndexObjectID = this.getMandatoryProperty(JSONUtils.BAM_TUMOUR_INDEX_OBJECT_ID);

			this.sangerGnosID = this.getMandatoryProperty(JSONUtils.SANGER_GNOS_ID);
			this.broadGnosID = this.getMandatoryProperty(JSONUtils.BROAD_GNOS_ID);
			this.dkfzemblGnosID = this.getMandatoryProperty(JSONUtils.DKFZEMBL_GNOS_ID);
			this.museGnosID = this.getMandatoryProperty(JSONUtils.MUSE_GNOS_ID);
			
			this.normalBamGnosID= this.getMandatoryProperty(JSONUtils.BAM_NORMAL_GNOS_ID);
			this.tumourBamGnosID= this.getMandatoryProperty(JSONUtils.BAM_TUMOUR_GNOS_ID);
			
			this.uploadKey= this.getMandatoryProperty("uploadKey");
			
			if (hasPropertyAndNotNull("gitMoveTestMode")) {
				this.gitMoveTestMode = Boolean.valueOf(getProperty("gitMoveTestMode"));
			}
			
			if (hasPropertyAndNotNull("storageSource")) {
				this.storageSource = getProperty("storageSource");
			}
			
			if (hasPropertyAndNotNull("snvPadding")) {
				this.snvPadding = Integer.valueOf(getProperty("snvPadding"));
			}
			
			if (hasPropertyAndNotNull("svPadding")) {
				this.svPadding = Integer.valueOf(getProperty("svPadding"));
			}
			
			if (hasPropertyAndNotNull("indelPadding")) {
				this.indelPadding = Integer.valueOf(getProperty("indelPadding"));
			}
			
			if (hasPropertyAndNotNull("refFile")) {
				this.refFile = getProperty("refFile");
			}
			
			if (hasPropertyAndNotNull("skipDownload")) {
				this.skipDownload = Boolean.valueOf(getProperty("skipDownload"));
			}
			
			if (hasPropertyAndNotNull("skipUpload")) {
				this.skipUpload = Boolean.valueOf(getProperty("skipUpload"));
			}
			
			if (hasPropertyAndNotNull("skipOxoG")) {
				this.skipOxoG = Boolean.valueOf(getProperty("skipOxoG"));
			}
			
			if (hasPropertyAndNotNull("skipVariantBam")) {
				this.skipVariantBam = Boolean.valueOf(getProperty("skipVariantBam"));
			}
			
			if (hasPropertyAndNotNull("skipAnnotation")) {
				this.skipVariantBam = Boolean.valueOf(getProperty("skipAnnotation"));
			}
			
			//this.generateRulesFile();
			
		} catch (Exception e) {
			throw new RuntimeException("Exception encountered during workflow init: "+e.getMessage(),e);
		}
	}
}
