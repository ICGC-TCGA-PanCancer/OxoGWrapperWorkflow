package com.github.seqware;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import com.github.seqware.OxoGWrapperWorkflow.DownloadMethod;
import com.github.seqware.OxoGWrapperWorkflow.Pipeline;
import com.github.seqware.OxoGWrapperWorkflow.VCFType;

import net.sourceforge.seqware.pipeline.workflowV2.AbstractWorkflowDataModel;

public abstract class BaseOxoGWrapperWorkflow extends AbstractWorkflowDataModel {
	
	//ugh... so many fields. There's probably a better way to do this, just no time right now.
	protected String oxoQScore = "";
	protected String donorID;
	protected String specimenID;
	protected String normalAliquotID;
	protected String bamNormalObjectID;
	protected String bamNormalIndexObjectID;
	protected String bamTumourObjectID;
	protected String bamTumourIndexObjectID;
	
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
	protected String normalMetdataURL;
	
	protected String normalBAMFileName;
	
	protected int snvPadding = 10;
	protected int svPadding = 500;
	protected int indelPadding = 200;
	
	protected String storageSource = "collab";
	
	protected boolean gitMoveTestMode = false;
	
	protected List<String> filesForUpload = new ArrayList<String>();
	

	protected String sangerGnosID;
	protected String broadGnosID;
	protected String dkfzemblGnosID;
	protected String museGnosID;
	
	//This could be used implement a sort of local-file mode. For now, it's just used to speed up testing.
	protected boolean skipDownload = false;
	protected boolean skipUpload = false;
	
	//Path to reference file usd for normalization, *relative* to /refdata/
	protected String refFile = "public/Homo_sapiens_assembly19.fasta";
	//protected String tumourBamGnosID;
	protected String normalBamGnosID;
	protected String uploadKey;
	protected String gnosKey;
	
	//These two variables can be used to skip running OxoG and variant bam, in case they have already been run.
	//Intended to speed up testing by skipping these steps when results already exist.
	protected boolean skipOxoG = false;
	protected boolean skipVariantBam = false;
	protected boolean skipAnnotation = false;
	
	protected String normalMinibamPath;
	//protected String tumourMinibamPath;
	
	protected String gnosMetadataUploadURL = "https://gtrepo-osdc-icgc.annailabs.com";
	
	protected String studyRefNameOverride = "icgc_pancancer_vcf";
	
	protected final String changelogURL = "https://github.com/ICGC-TCGA-PanCancer/OxoGWrapperWorkflow/CHANGELOG.md";
	
	protected final String workflowSourceURL = "https://github.com/ICGC-TCGA-PanCancer/OxoGWrapperWorkflow/";
	
	protected final String workflowURL = "https://github.com/ICGC-TCGA-PanCancer/OxoGWrapperWorkflow/";
	
	protected String downloadMethod = "icgcStorageClient";
	
	protected String sangerGNOSRepoURL;
	protected String broadGNOSRepoURL;
	protected String dkfzEmblGNOSRepoURL;
	protected String museGNOSRepoURL;
	protected String normalBamGNOSRepoURL;
	
	protected String normalBamIndexFileName;
	
	protected Map<String,String> objectToFilenames = new HashMap<String,String>(26);
	
	protected Map<String,String> workflowNamestoGnosIds = new HashMap<String,String>(6);
	
	protected String icgcStorageClientVersion = "latest";
	protected String compbioNgseasyBaseVersion = "a1.0-002";
	protected String pancancerUploadDownloadVersion = "1.7";
	protected String pcawgAnnotator = "latest";
	
	protected String gtDownloadBamKey = "";
	protected String gtDownloadVcfKey = "";
	
	protected int tumourCount;
	
	List<TumourInfo> tumours;

	protected boolean allowMissingFiles = false;
	
	List<VcfInfo> vcfs;

	/**
	 * Get a property name that is mandatory
	 * @param propName The name of the property
	 * @return The property, as a String. Convert to other types if you need to.
	 * @throws Exception An Exception with the message "Property with key <i>propName</i> cannot be null" will be thrown if property is not found.
	 */
	private String getMandatoryProperty(String propName)
	{
		if (hasPropertyAndNotNull(propName)) {
			try {
				return getProperty(propName);
			}
			catch (Exception e)
			{
				throw new RuntimeException (e);
			}
		}
		else {
			throw new RuntimeException ("Property with key "+propName+ " cannot be null!");
		}
	}
	
	private String getOptionalProperty(String propName)
	{
		boolean hasPropertyAndNotNull = hasPropertyAndNotNull(propName);
		if (hasPropertyAndNotNull)
		{
			try
			{
				return getProperty(propName);
			}
			catch (Exception e)
			{
				throw new RuntimeException("The property "+propName+ " seems to be in the INI file: "+hasPropertyAndNotNull + ", but retrieving the property caused an exception, which shouldn't have happened!",e);
			}
		}
		return "";
	}

	/**
	 * Initial setup.
	 */
	protected void init() {
		//Was having class-loader issues with Jinja, probably coming from however it is that seqware builds these bundles, this seems to have resolved it.
		Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());

		// If missing files are NOT allowed, an exception should be thrown for missing values. Otherwise, 
		// fields will be set to either the value in the INI file, or an empty string, with no exceptions being thrown.
		Function<String, String> setBasedOnAllowMissingFiles = (lookupKey) -> {
				return ( ( ! this.allowMissingFiles)
						 ? this.getMandatoryProperty(lookupKey)
						 : this.getOptionalProperty(lookupKey) );
		};

		BiConsumer<String, String> putIntoMapIfSet = (id, name) -> { 
			if (name !=null && !name.trim().equals(""))
			{
				this.objectToFilenames.put(id, name);
			}
		} ;

		try {
			if (hasPropertyAndNotNull("downloadMethod")) {
				this.downloadMethod = getProperty("downloadMethod");
			}
			
			this.tumourCount = Integer.valueOf(this.getMandatoryProperty(JSONUtils.TUMOUR_COUNT));
			
			this.tumours = new ArrayList<TumourInfo>(tumourCount);
			this.vcfs = new ArrayList<VcfInfo>((tumourCount * 3)+1);
			
			this.sangerGnosID = this.getMandatoryProperty(JSONUtils.SANGER_GNOS_ID);
			this.broadGnosID = this.getMandatoryProperty(JSONUtils.BROAD_GNOS_ID);
			this.dkfzemblGnosID = this.getMandatoryProperty(JSONUtils.DKFZEMBL_GNOS_ID);
			this.museGnosID = this.getMandatoryProperty(JSONUtils.MUSE_GNOS_ID);

			if (hasPropertyAndNotNull("allowMissingFiles")) {
				this.allowMissingFiles = Boolean.valueOf(getProperty("allowMissingFiles"));
			}
			System.out.println("DEBUG: allowMissingFiles: "+allowMissingFiles);
			for (int i = 0; i < tumourCount; i++)
			{
				System.out.println("reading tumour "+i);
				TumourInfo tInf = new TumourInfo();
				tInf.setTumourMetdataURL( this.getMandatoryProperty(JSONUtils.BAM_TUMOUR_METADATA_URL+"_"+i));
				tInf.setTumourBAMFileName(this.getMandatoryProperty(JSONUtils.BAM_TUMOUR_FILE_NAME+"_"+i));
				tInf.setTumourBamGnosID( this.getMandatoryProperty(JSONUtils.BAM_TUMOUR_GNOS_ID+"_"+i));
				tInf.setTumourBamIndexFileName ( this.getMandatoryProperty(JSONUtils.TUMOUR_BAM_INDEX_FILE_NAME+"_"+i));
				tInf.setBamTumourObjectID  (this.getMandatoryProperty(JSONUtils.BAM_TUMOUR_OBJECT_ID+"_"+i));
				tInf.setBamTumourIndexObjectID (this.getMandatoryProperty(JSONUtils.BAM_TUMOUR_INDEX_OBJECT_ID+"_"+i));
				tInf.setAliquotID(this.getMandatoryProperty(JSONUtils.TUMOUR_ALIQUOT_ID+"_"+i));
				this.workflowNamestoGnosIds.put(OxoGWrapperWorkflow.BAMType.tumour.toString()+"_"+i, tInf.getTumourBamGnosID());
				this.objectToFilenames.put(tInf.getBamTumourObjectID(), tInf.getTumourBAMFileName());
				this.objectToFilenames.put(tInf.getBamTumourIndexObjectID(), tInf.getTumourBamIndexFileName());
				
				if (this.downloadMethod != null && !this.downloadMethod.equals("") && this.downloadMethod.equals(DownloadMethod.gtdownload.toString()))
				{
					tInf.setTumourBamGNOSRepoURL ( this.getMandatoryProperty(JSONUtils.TUMOUR_BAM_DOWNLOAD_URL+"_"+i) );
				}
				this.tumours.add(tInf);
				
				
				for (Pipeline p : Pipeline.values())
				{
					for (VCFType v : VCFType.values())
					{
						if (p == Pipeline.muse && v != VCFType.snv)
						{
							//do nothing when MUSE and non-SNV, because MUSE will ONLY have SNV.
						}
						else
						{
							VcfInfo vInfo = new VcfInfo();
							vInfo.setObjectID( setBasedOnAllowMissingFiles.apply(JSONUtils.lookUpKeyGenerator(p, v, "data", "object_id_"+i)));
							vInfo.setIndexObjectID( setBasedOnAllowMissingFiles.apply(JSONUtils.lookUpKeyGenerator(p, v, "index", "object_id_"+i)));
							vInfo.setFileName( setBasedOnAllowMissingFiles.apply(JSONUtils.lookUpKeyGenerator(p, v, "data", "file_name_"+i)));
							vInfo.setIndexFileName( setBasedOnAllowMissingFiles.apply(JSONUtils.lookUpKeyGenerator(p, v, "index", "file_name_"+i)));
							vInfo.setOriginatingTumourAliquotID( setBasedOnAllowMissingFiles.apply(JSONUtils.TUMOUR_ALIQUOT_ID+"_"+i));
							vInfo.setVcfType(v);
							vInfo.setOriginatingPipeline(p);
							
							putIntoMapIfSet.accept(vInfo.getObjectID(),vInfo.getFileName());
							putIntoMapIfSet.accept(vInfo.getIndexObjectID(),vInfo.getIndexFileName());
							switch (p)
							{
								case broad:
									vInfo.setPipelineGnosID(this.broadGnosID);
									break;
								case dkfz_embl:
									vInfo.setPipelineGnosID(this.dkfzemblGnosID);
									break;
								case sanger:
									vInfo.setPipelineGnosID(this.sangerGnosID);
									break;
								case muse:
									vInfo.setPipelineGnosID(this.museGnosID);
									break;
							}
							//Only add this object if there is an actual file name present.
							if (vInfo.getFileName()!=null && !vInfo.getFileName().trim().equals(""))
							{
								this.vcfs.add(vInfo);
							}
						}
					}
				}
			}
			
			this.oxoQScore = this.getMandatoryProperty(JSONUtils.OXOQ_SCORE);
			this.JSONrepo = this.getMandatoryProperty("JSONrepo");
			this.JSONrepoName = this.getMandatoryProperty("JSONrepoName");
			this.JSONfolderName = this.getMandatoryProperty("JSONfolderName");
			this.JSONfileName = this.getMandatoryProperty("JSONfileName");
			this.GITemail = this.getMandatoryProperty("GITemail");
			this.GITname = this.getMandatoryProperty("GITname");
			
			this.donorID = this.getMandatoryProperty(JSONUtils.SUBMITTER_DONOR_ID);
			this.specimenID = this.getMandatoryProperty(JSONUtils.SUBMITTER_SPECIMENT_ID);
			
			this.bamNormalObjectID = this.getMandatoryProperty(JSONUtils.BAM_NORMAL_OBJECT_ID);
			this.normalMetdataURL = this.getMandatoryProperty(JSONUtils.BAM_NORMAL_METADATA_URL);
			this.uploadURL = this.getMandatoryProperty("uploadURL");
			this.normalAliquotID = this.getMandatoryProperty(JSONUtils.NORMAL_ALIQUOT_ID);
			
			this.GITPemFile = this.getMandatoryProperty("GITPemFile");

			this.normalBAMFileName = this.getMandatoryProperty(JSONUtils.BAM_NORMAL_FILE_NAME);
			
			this.bamNormalIndexObjectID = this.getMandatoryProperty(JSONUtils.BAM_NORMAL_INDEX_OBJECT_ID);
			
			this.normalBamGnosID= this.getMandatoryProperty(JSONUtils.BAM_NORMAL_GNOS_ID);
			
			this.workflowNamestoGnosIds.put(OxoGWrapperWorkflow.Pipeline.sanger.toString(), this.sangerGnosID);
			this.workflowNamestoGnosIds.put(OxoGWrapperWorkflow.Pipeline.broad.toString(), this.broadGnosID);
			this.workflowNamestoGnosIds.put(OxoGWrapperWorkflow.Pipeline.dkfz_embl.toString(), this.dkfzemblGnosID);
			this.workflowNamestoGnosIds.put(OxoGWrapperWorkflow.Pipeline.muse.toString(), this.museGnosID);
			this.workflowNamestoGnosIds.put(OxoGWrapperWorkflow.BAMType.normal.toString(), this.normalBamGnosID);

			this.normalBamIndexFileName = this.getMandatoryProperty(JSONUtils.NORMAL_BAM_INDEX_FILE_NAME);
			
			this.objectToFilenames.put(this.bamNormalObjectID, this.normalBAMFileName);
			this.objectToFilenames.put(this.bamNormalIndexObjectID, this.normalBamIndexFileName);
			this.uploadKey= this.getMandatoryProperty("uploadKey");
			this.gnosKey= this.getMandatoryProperty("gnosKey");
			
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
				this.skipAnnotation = Boolean.valueOf(getProperty("skipAnnotation"));
			}
			
			if (hasPropertyAndNotNull("gnosMetadataUploadURL")) {
				this.gnosMetadataUploadURL = getProperty("gnosMetadataUploadURL");
			}

			if (hasPropertyAndNotNull("studyRefNameOverride")) {
				this.studyRefNameOverride = getProperty("studyRefNameOverride");
			}
			
			//System.out.println("DEBUG: downloadMethod: "+this.downloadMethod);
			if (this.downloadMethod != null && !this.downloadMethod.equals("") && this.downloadMethod.equals(DownloadMethod.gtdownload.toString()))
			{
				System.out.println("DEBUG: Setting gtdownload-specific config values");
				this.sangerGNOSRepoURL = this.getMandatoryProperty(JSONUtils.SANGER_DOWNLOAD_URL);
				this.broadGNOSRepoURL = this.getMandatoryProperty(JSONUtils.BROAD_DOWNLOAD_URL);
				this.dkfzEmblGNOSRepoURL = this.getMandatoryProperty(JSONUtils.DKFZ_EMBL_DOWNLOAD_URL);
				this.museGNOSRepoURL = this.getMandatoryProperty(JSONUtils.MUSE_DOWNLOAD_URL);
				this.normalBamGNOSRepoURL = this.getMandatoryProperty(JSONUtils.NORMAL_BAM_DOWNLOAD_URL);
				this.gtDownloadBamKey = this.getMandatoryProperty("gtDownloadBamKey");
				this.gtDownloadVcfKey = this.getMandatoryProperty("gtDownloadVcfKey");
			}
			
		} catch (Exception e) {
			throw new RuntimeException("Exception encountered during workflow init: "+e.getMessage(),e);
		}
	}
}
