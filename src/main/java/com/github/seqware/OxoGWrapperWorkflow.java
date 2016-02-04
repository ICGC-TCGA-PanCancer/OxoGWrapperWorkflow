package com.github.seqware;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import net.sourceforge.seqware.pipeline.workflowV2.AbstractWorkflowDataModel;
import net.sourceforge.seqware.pipeline.workflowV2.model.Job;

public class OxoGWrapperWorkflow extends AbstractWorkflowDataModel {

	private String oxoQScore = "";
	// private String donorID;
	private String aliquotID;
	private String bamNormalObjectID;
	private String bamNormalIndexObjectID;
	private String bamTumourObjectID;
	private String bamTumourIndexObjectID;
	
	private String sangerSNVVCFObjectID;
	private String dkfzemblSNVVCFObjectID;
	private String broadSNVVCFObjectID;
	private String museSNVVCFObjectID;

	private String sangerIndelVCFObjectID;
	private String dkfzemblIndelVCFObjectID;
	private String broadIndelVCFObjectID;

	private String sangerSVVCFObjectID;
	private String dkfzemblSVVCFObjectID;
	private String broadSVVCFObjectID;

	private String sangerSNVIndexObjectID;
	private String dkfzemblSNVIndexObjectID;
	private String broadSNVIndexObjectID;
	private String museSNVIndexObjectID;

	private String sangerIndelIndexObjectID;
	private String dkfzemblIndelIndexObjectID;
	private String broadIndelIndexObjectID;

	private String sangerSVIndexObjectID;
	private String dkfzemblSVIndexObjectID;
	private String broadSVIndexObjectID;
	
	
	private String uploadURL;

	private String JSONrepo = null;
	private String JSONrepoName = "oxog-ops";
	private String JSONfolderName = null;
	private String JSONlocation = "/datastore/gitroot";
	private String JSONfileName = null;

	private String GITemail = "";
	private String GITname = "ICGC AUTOMATION";
	private String GITPemFile = "";

	//These will be needed so that vcf-uploader can generate the analysis.xml and manifest.xml files
	private String tumourMetdataURL;
	private String normalMetdataURL;
	
	private String tumourBAMFileName;
	private String normalBAMFileName;
	private String sangerSNVName;
	private String dkfzEmblSNVName;
	private String broadSNVName;
	private String museSNVName;

	private String sangerSVName;
	private String dkfzEmblSVName;
	private String broadSVName;

	private String sangerIndelName;
	private String dkfzEmblIndelName;
	private String broadIndelName;
	
	private int snvPadding = 10;
	private int svPadding = 10;
	private int indelPadding = 200;
	
	private String storageSource = "collab";
	
	private boolean gitMoveTestMode = false;
	
	/* # of files to upload:
	 * 1 OxoG SNV
	 * 	1 index file
	 * 2 minibams
	 * 	2 index files
	 * 3 normalized indels
	 * 	3 index files
	 * 1 OxoG SNV from indels
	 * 	1 index
	 * 3 SV files
	 * 	3 index files
	 * ... and then some from Jonathan's annotation process... (TBD)
	 */
	private List<String> filesToUpload = new ArrayList<String>(25);
	
	//Paths to VCFs generated by merging types across workflows. 
	private String snvVCF;
	private String svVCF;
	private String indelVCF;
	private String sangerNormalizedIndelVCFName;
	private String broadNormalizedIndelVCFName;
	private String dkfzEmblNormalizedIndelVCFName;
	//private String museNormalizedIndelVCFName;

	private String sangerExtractedSNVVCFName;
	private String broadExtractedSNVVCFName;
	private String dkfzEmblExtractedSNVVCFName;
	private String museExtractedSNVVCFName;

	
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
			this.dkfzEmblSNVName = this.getMandatoryProperty(JSONUtils.DFKZEMBL_SNV_VCF_NAME);
			this.museSNVName = this.getMandatoryProperty(JSONUtils.MUSE_VCF_NAME);
			
			this.sangerIndelName = this.getMandatoryProperty(JSONUtils.SANGER_INDEL_VCF_NAME);
			this.dkfzEmblIndelName = this.getMandatoryProperty(JSONUtils.DFKZEMBL_INDEL_VCF_NAME);
			this.broadIndelName = this.getMandatoryProperty(JSONUtils.BROAD_INDEL_VCF_NAME);

			this.sangerSVName = this.getMandatoryProperty(JSONUtils.SANGER_SV_VCF_NAME);
			this.dkfzEmblSVName = this.getMandatoryProperty(JSONUtils.DFKZEMBL_SV_VCF_NAME);
			this.broadSVName = this.getMandatoryProperty(JSONUtils.BROAD_SV_VCF_NAME);
			
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

			
			if (hasPropertyAndNotNull("gitMoveTestMode")) {
				//gitMoveTestMode is not mandatory - it should default to false.
				this.gitMoveTestMode = Boolean.valueOf(getProperty("gitMoveTestMode"));
			}
			
			if (hasPropertyAndNotNull("storageSource")) {
				//storageSource is not mandatory - it should default to "collab"
				this.storageSource = getProperty("storageSource");
			}
			
			if (hasPropertyAndNotNull("snvPadding")) {
				//snv padding is not mandatory
				this.snvPadding = Integer.valueOf(getProperty("snvPadding"));
			}
			
			if (hasPropertyAndNotNull("svPadding")) {
				//sv padding is not mandatory
				this.svPadding = Integer.valueOf(getProperty("svPadding"));
			}
			
			if (hasPropertyAndNotNull("indelPadding")) {
				//indel padding is not mandatory
				this.indelPadding = Integer.valueOf(getProperty("indelPadding"));
			}
			
			this.generateRulesFile();
			
		} catch (Exception e) {
			throw new RuntimeException("Exception encountered during workflow init: "+e.getMessage(),e);
		}
	}

	/**
	 * Generates a rules file that is used for the variant program that produces minibams.
	 * @throws URISyntaxException
	 * @throws IOException
	 */
	private void generateRulesFile() throws URISyntaxException, IOException
	{
		Path pathToPaddingRules = Paths.get(new URI("file:////datastore/padding_rules.txt"));
		String paddingFileString = "pad["+this.svPadding+"];mlregion@/sv.vcf\n"+
									"pad["+this.snvPadding+"];mlregion@/snv.vcf\n"+
									"pad["+this.indelPadding+"];mlregion@/indel.vcf\n";
		
		Files.write(pathToPaddingRules, paddingFileString.getBytes(), StandardOpenOption.CREATE);
	}
	
	/**
	 * Copy the credentials files from ~/.gnos to /datastore/credentials
	 * @param parentJob
	 * @return
	 */
	private Job copyCredentials(Job parentJob){
		//Might need to set transport.parallel to some fraction of available cores for icgc-storage-client. Use this command to get # CPUs.
		//The include it in the collab.token file since that's what gets mounted to /icgc/icgc-storage-client/conf/application.properties
		//lscpu | grep "^CPU(s):" | grep -o "[^ ]$"
		//Andy says transport.parallel is not yet supported, but transport.memory may improve performance.
		//Also set transport.memory: either "4" or "6" (GB - implied). 
		Job copy = this.getWorkflow().createBashJob("copy ~/.gnos");
		copy.setCommand("sudo cp -r ~/.gnos /datastore/credentials && ls -l /datastore/credentials");
		copy.addParent(parentJob);
		return copy;
	}
	
	/**
	 * Defines what BAM types there are:
	 * <ul><li>normal</li><li>tumour</li></ul>
	 * @author sshorser
	 *
	 */
	enum BAMType{
		normal,tumour
	}
	/**
	 * Download a BAM file.
	 * @param parentJob
	 * @param objectID - the object ID of the BAM file
	 * @param bamType - is it normal or tumour? This used to determine the name of the directory that the file ends up in.
	 * @return
	 */
	private Job getBAM(Job parentJob, BAMType bamType, String ... objectIDs) {
		Job getBamFileJob = this.getWorkflow().createBashJob("get "+bamType.toString()+" BAM file");
		getBamFileJob.addParent(parentJob);
		
		String downloadObjects = "";
		
		for (String objectID : objectIDs)
		{
			downloadObjects += "/icgc/icgc-storage-client/bin/icgc-storage-client url --object-id "+objectID+" ;\n" 
			+ " /icgc/icgc-storage-client/bin/icgc-storage-client download --object-id "
				+ objectID +" --output-layout bundle --output-dir /downloads/ ;\n";
		}
		String storageClientDockerCmdNormal ="sudo docker run --rm --name get_bam_"+bamType+" "
				+ " -e STORAGE_PROFILE="+this.storageSource+" " 
			    + " -v /datastore/bam/"+bamType.toString()+"/logs/:/icgc/icgc-storage-client/logs/:rw "
				+ " -v /datastore/credentials/collab.token:/icgc/icgc-storage-client/conf/application.properties:ro "
			    + " -v /datastore/bam/"+bamType.toString()+"/:/downloads/:rw"
	    		+ " icgc/icgc-storage-client /bin/bash -c "
	    		+ " \" "+downloadObjects+" \"";
		getBamFileJob.setCommand(storageClientDockerCmdNormal);

		return getBamFileJob;
	}

	/**
	 * Defines the different pipelines:
	 * <ul>
	 * <li>sanger</li>
	 * <li>dkfz_embl</li>
	 * <li>broad</li>
	 * <li>muse</li>
	 * </ul>
	 * @author sshorser
	 *
	 */
	enum Pipeline {
		sanger, dkfz_embl, broad, muse
	}
	/**
	 This will download VCFs for a workflow, based on an object ID(s).
	 It will perform these operations:
	 <ol>
	 <li>download VCFs</li>
	 <li>normalize INDEL VCF</li>
	 <li>extract SNVs from INDEL into a separate VCF</li>
	 </ol>
	 * @param parentJob
	 * @param workflowName The pipeline (AKA workflow) that the VCFs come from. This will determine the name of the output directory where the downloaded files will be stored.
	 * @param objectID
	 * @return
	 */
	private Job getVCF(Job parentJob, Pipeline workflowName, String ... objectIDs) {
		Job getVCFJob = this.getWorkflow().createBashJob("get VCF for workflow " + workflowName);
		String outDir = "/datastore/vcf/"+workflowName;
		String downloadObjects = "";
		for (String objectID : objectIDs)
		{
			downloadObjects += " /icgc/icgc-storage-client/bin/icgc-storage-client url --object-id "+objectID+" ;\n" 
				+ " /icgc/icgc-storage-client/bin/icgc-storage-client download --object-id " + objectID+" --output-layout bundle --output-dir /downloads/ ;\n "; 
		}
		
		String getVCFCommand = "sudo docker run --rm --name get_vcf_"+workflowName+" "
				+ " -e STORAGE_PROFILE="+this.storageSource+" " 
			    + " -v "+outDir+"/logs/:/icgc/icgc-storage-client/logs/:rw "
				+ " -v /datastore/credentials/collab.token:/icgc/icgc-storage-client/conf/application.properties:ro "
			    + " -v "+outDir+"/:/downloads/:rw"
	    		+ " icgc/icgc-storage-client /bin/bash -c \" "+downloadObjects+" \" ";
		
		getVCFJob.setCommand(getVCFCommand);
		getVCFJob.addParent(parentJob);



		return getVCFJob;
	}

	/**
	 * Pre-processes VCFs. Normalizes INDELs and extracts SNVs from normalized INDELs.
	 * @param parent
	 * @param workflowName The name of the workflow whose files will be pre-processed.
	 * @param vcfName The name of the INDEL VCF to normalize.
	 * @return
	 */
	private Job preProcessVCF(Job parent, Pipeline workflowName, String vcfName )
	{
		String outDir = "/datastore/vcf/"+workflowName;
		String normalizedINDELName = workflowName+"_somatic.indel.bcftools-norm.vcf.gz";
		String extractedSNVVCFName = workflowName+"_somatic.indel.bcftools-norm.extracted-snvs.vcf";
		// TODO: Many of these steps below could probably be combined into a single Job
		// that makes runs a single docker container, but executes multiple commands.
		Job bcfToolsNormJob = this.getWorkflow().createBashJob("run VCF primitives on indel");
		String runBCFToolsNormCommand = "sudo docker run --rm --name normalize_indel_"+workflowName+" "
					+ " -v "+outDir+"/"+vcfName+":/datastore/datafile.vcf.gz "
					+ " -v /datastore/refdata/public:/ref"
					+ " compbio/ngseasy-base:a1.0-002 " 
					+ " bcftools norm -c w -m -any -O -z -f /ref/Homo_sapiens_assembly19.fasta  /datastore/datafile.vcf.gz "  
				+ " > "+outDir+"/"+normalizedINDELName
				+ " && tabix -f -p vcf "+outDir+"/"+normalizedINDELName;
		bcfToolsNormJob.setCommand(runBCFToolsNormCommand);
		bcfToolsNormJob.addParent(parent);
		
		//Normalized INDELs should be indexed uploaded
		
		filesToUpload.add(outDir+"/"+normalizedINDELName);
		filesToUpload.add(outDir+"/"+normalizedINDELName+".tbi");
		
		Job extractSNVFromIndel = this.getWorkflow().createBashJob("extracting SNVs from INDEL");
		extractSNVFromIndel.setCommand("bgzip -d -c "+outDir+"/"+normalizedINDELName+" > "+outDir+"/"+workflowName+"_somatic.indel.bcftools-norm.vcf "
				+ "							&& grep -e '^#' -i -e '^[^#].*[[:space:]][ACTG][[:space:]][ACTG][[:space:]]' "+outDir+"/somatic.indel.bcftools-norm.vcf "
											+ "> "+outDir+"/"+extractedSNVVCFName
												+ " && bgzip "+outDir+"/"+extractedSNVVCFName
												+ " && tabix -f -p vcf "+outDir+"/"+extractedSNVVCFName);
		extractSNVFromIndel.addParent(bcfToolsNormJob);
		
		switch (workflowName) {
			case sanger:
				this.sangerNormalizedIndelVCFName = outDir + "/"+normalizedINDELName;
				this.sangerExtractedSNVVCFName = outDir + "/"+extractedSNVVCFName;
				break;
			case broad:
				this.broadNormalizedIndelVCFName = outDir + "/"+normalizedINDELName;
				this.broadExtractedSNVVCFName = outDir + "/"+extractedSNVVCFName;
				break;
			case dkfz_embl:
				this.dkfzEmblNormalizedIndelVCFName = outDir + "/"+normalizedINDELName;
				this.dkfzEmblExtractedSNVVCFName = outDir + "/"+extractedSNVVCFName;
				break;
//			case muse:
//				this.museNormalizedIndelVCFName = outDir + "/"+normalizedINDELName;
//				this.museExtractedSNVVCFName = outDir + "/"+extractedSNVVCFName;
//				break;
			default:
				// Just in case someone adds a new pipeline and then doesn't write code to handle it.
				throw new RuntimeException("Unknown pipeline: "+workflowName);
		}
	
		return extractSNVFromIndel;
	}
	
	/**
	 * The types of VCF files there are:
	 * <ul>
	 * <li>sv</li>
	 * <li>snv</li>
	 * <li>indel</li>
	 * </ul>
	 * @author sshorser
	 *
	 */
	enum VCFType{
		sv, snv, indel
	}
	/**
	 * This will combine VCFs from different workflows by the same type. All INDELs will be combined into a new output file,
	 * all SVs will be combined into a new file, all SNVs will be combined into a new file. 
	 * @param parents
	 * @return
	 */
	private Job combineVCFsByType(Job ... parents)
	{
		//Create symlinks to the files in the proper directory.
		Job prepVCFs = this.getWorkflow().createBashJob("Create links to VCFs");
		prepVCFs.getCommand().addArgument("\n sudo mkdir /datastore/merged_vcfs/ && sudo chmod a+rw /datastore/merged_vcfs ; \n");
		prepVCFs.getCommand().addArgument("\n ln -s /datastore/vcf/"+Pipeline.sanger+"/"+this.sangerSNVName+" /datastore/vcf/"+Pipeline.sanger+"_snv.vcf ; \n");
		prepVCFs.getCommand().addArgument(" ln -s /datastore/vcf/"+Pipeline.broad+"/"+this.broadSNVName+" /datastore/vcf/"+Pipeline.broad+"_snv.vcf ; \n");
		prepVCFs.getCommand().addArgument(" ln -s /datastore/vcf/"+Pipeline.dkfz_embl+"/"+this.sangerSNVName+" /datastore/vcf/"+Pipeline.dkfz_embl+"_snv.vcf ; \n");
		prepVCFs.getCommand().addArgument(" ln -s /datastore/vcf/"+Pipeline.muse+"/"+this.sangerSNVName+" /datastore/vcf/"+Pipeline.muse+"_snv.vcf ; \n");

		prepVCFs.getCommand().addArgument(" ln -s /datastore/vcf/"+Pipeline.sanger+"/"+this.sangerNormalizedIndelVCFName+" /datastore/vcf/"+Pipeline.sanger+"_indel.vcf ; \n");
		prepVCFs.getCommand().addArgument(" ln -s /datastore/vcf/"+Pipeline.broad+"/"+this.broadNormalizedIndelVCFName+" /datastore/vcf/"+Pipeline.broad+"_indel.vcf ; \n");
		prepVCFs.getCommand().addArgument(" ln -s /datastore/vcf/"+Pipeline.dkfz_embl+"/"+this.dkfzEmblNormalizedIndelVCFName+" /datastore/vcf/"+Pipeline.dkfz_embl+"_indel.vcf ; \n");

		prepVCFs.getCommand().addArgument(" ln -s /datastore/vcf/"+Pipeline.sanger+"/"+this.sangerSVName+" /datastore/vcf/"+Pipeline.sanger+"_sv.vcf ; \n");
		prepVCFs.getCommand().addArgument(" ln -s /datastore/vcf/"+Pipeline.broad+"/"+this.broadSVName+" /datastore/vcf/"+Pipeline.broad+"_sv.vcf ; \n");
		prepVCFs.getCommand().addArgument(" ln -s /datastore/vcf/"+Pipeline.dkfz_embl+"/"+this.dkfzEmblSVName+" /datastore/vcf/"+Pipeline.dkfz_embl+"_sv.vcf ; \n");
		
		for (Job parent : parents)
		{
			prepVCFs.addParent(parent);
		}
		
		Job vcfCombineJob = this.getWorkflow().createBashJob("Combining VCFs by type");
		
		//run the merge script, then bgzip and index them all.
		vcfCombineJob.setCommand("perl "+this.getWorkflowBaseDir()+"/scripts/vcf_merge_by_type.pl "
				+ Pipeline.broad+"_snv.vcf "+Pipeline.sanger+"_snv.vcf "+Pipeline.dkfz_embl+"_snv.vcf "+Pipeline.muse+"_snv.vcf "
				+ Pipeline.broad+"_indel.vcf "+Pipeline.sanger+"_indel.vcf "+Pipeline.dkfz_embl+"_indel.vcf" 
				+ Pipeline.broad+"_sv.vcf "+Pipeline.sanger+"_sv.vcf "+Pipeline.dkfz_embl+"_sv.vcf"
				+ " /datastore/vcf/ /datastore/merged_vcfs/ ; \n"
				+ " bgzip -c /datastore/merged_vcfs/snv.clean.sorted.vcf > /datastore/merged_vcfs/snv.clean.sorted.vcf.gz ; \n"
				+ " bgzip -c /datastore/merged_vcfs/sv.clean.sorted.vcf > /datastore/merged_vcfs/sv.clean.sorted.vcf.gz ; \n"
				+ " bgzip -c /datastore/merged_vcfs/indel.clean.sorted.vcf > /datastore/merged_vcfs/indel.clean.sorted.vcf.gz ; \n"
				+ " tabix -f /datastore/merged_vcfs/snv.clean.sorted.vcf.gz ; \n"
				+ " tabix -f /datastore/merged_vcfs/sv.clean.sorted.vcf.gz ; \n"
				+ " tabix -f /datastore/merged_vcfs/indel.clean.sorted.vcf.gz ; \n");
		
		
		vcfCombineJob.addParent(prepVCFs);
		this.snvVCF = "/datastore/merged_vcfs/snv.clean.sorted.vcf";
		this.svVCF = "/datastore/merged_vcfs/sv.clean.sorted.vcf";
		this.indelVCF = "/datastore/merged_vcfs/indel.clean.sorted.vcf";
		
		return vcfCombineJob;
	}
	
	/**
	 * Runs the OxoG filtering program inside the Broad's OxoG docker container. Output file(s) will be in /datastore/oxog_results/ and the working files will 
	 * be in /datastore/oxog_workspace
	 * @param parent
	 * @return
	 */
	private Job doOxoG(Job parent) {
		Job runOxoGWorkflow = this.getWorkflow().createBashJob("Run OxoG Filter");
		String oxogMounts = " -v /datastore/refdata/:/cga/fh/pcawg_pipeline/refdata/ "
				+ " -v /datastore/oncotator_db/:/cga/fh/pcawg_pipeline/refdata/public/oncotator_db/ "  
				+ " -v /datastore/oxog_workspace/:/cga/fh/pcawg_pipeline/jobResults_pipette/jobs/"+this.aliquotID+"/:rw " 
				+ " -v /datastore/bam/:/datafiles/BAM/  -v /datastore/vcf/:/datafiles/VCF/ "
				+ " -v /datastore/oxog_results/:/cga/fh/pcawg_pipeline/jobResults_pipette/results:rw ";
		String oxogCommand = "/cga/fh/pcawg_pipeline/pipelines/run_one_pipeline.bash pcawg /cga/fh/pcawg_pipeline/pipelines/oxog_pipeline.py "
				+ this.aliquotID + " " + this.tumourBAMFileName + " " + this.normalBAMFileName + " " + this.oxoQScore + " "
				+ this.sangerSNVName + " " + this.dkfzEmblSNVName + " " + this.broadSNVName;
		runOxoGWorkflow.setCommand(
				"sudo docker run --name=\"oxog_filter\" "+oxogMounts+" oxog " + oxogCommand);
		
		runOxoGWorkflow.addParent(parent);
		//Job getLogs = this.getOxoGLogs(runOxoGWorkflow);
		
		//TODO: will probably need to find a way to extract *just* the VCF (and the index - or create a new index) from this tar.
		this.filesToUpload.add("/datastore/oxog_results/*.gnos_files.tar");

		return runOxoGWorkflow;
	}

	/**
	 * This will run the OxoG Filter program on the SNVs that were extracted from the INDELs, if there were any. It's possible that no SNVs will be extracted from any
	 * INDEL files (in fact, I've been told this is the most likely scenario for most donors) in which case nothing will run. See the script scripts/run_oxog_extracted_SNVs.sh
	 * for more details on this.
	 * @param parent
	 * @return
	 */
	private Job doOxoGSnvsFromIndels(Job parent) {
		Job oxoGOnSnvsFromIndels = this.getWorkflow().createBashJob("Running OxoG on SNVs from INDELs");
		//String vcfBaseDir = "/datastore/vcf/";
		String vcf1 = this.sangerExtractedSNVVCFName; //vcfBaseDir+Pipeline.broad.toString()+"/somatic.indel.bcftools-norm.extracted-snvs.vcf ";
		String vcf2 = this.broadExtractedSNVVCFName; //vcfBaseDir+Pipeline.sanger.toString()+"/somatic.indel.bcftools-norm.extracted-snvs.vcf ";
		String vcf3 = this.dkfzEmblExtractedSNVVCFName; //vcfBaseDir+Pipeline.dkfz_embl.toString()+"/somatic.indel.bcftools-norm.extracted-snvs.vcf ";
		String vcf4 = this.museExtractedSNVVCFName; //vcfBaseDir+Pipeline.muse.toString()+"/somatic.indel.bcftools-norm.extracted-snvs.vcf ";
		oxoGOnSnvsFromIndels.setCommand(this.getWorkflowBaseDir()+"/scripts/run_oxog_extracted_SNVs.sh "+
																vcf1+" "+vcf2+" "+vcf3+" "+vcf4+" "+
																this.normalBAMFileName+" "+this.tumourBAMFileName+" "+
																this.aliquotID+" "+
																this.oxoQScore);
		oxoGOnSnvsFromIndels.addParent(parent);
		//TODO: will probably need to find a way to extract *just* the VCF from this tar.
		//Also, this one will be tricky: the file might not exist, but we can't determine that at 
		//workflow-build time - it will only be known once the scripts start running. Might need 
		//to add this to the upload programatically...
		this.filesToUpload.add("/datastore/oxog_results_extracted_snvs/*.gnos_files.tar");
		
		return oxoGOnSnvsFromIndels;
	}
	
	/**
	 * Runs the variant program inside the Broad's OxoG container to produce a mini-BAM for a given BAM. 
	 * @param parent
	 * @param bamType - The type of BAM file to use. Determines the name of the output file.
	 * @param bamPath - The path to the input BAM file.
	 * @return
	 */
	private Job doVariantBam(Job parent, BAMType bamType, String bamPath) {
		Job runOxoGWorkflow = this.getWorkflow().createBashJob("Run variantbam");

		String command = DockerCommandCreator.createVariantBamCommand(bamType, bamPath, this.snvVCF, this.svVCF, this.indelVCF);
		runOxoGWorkflow.setCommand(command);
		//The bam file will need to be indexed!
		runOxoGWorkflow.getCommand().addArgument("\nsamtools index /datastore/variantbam_results/minibam_"+bamType+".bam ; \n");
		
		this.filesToUpload.add("/datastore/variantbam_results/minibam_"+bamType+".bam");
		this.filesToUpload.add("/datastore/variantbam_results/minibam_"+bamType+".bai");
		runOxoGWorkflow.addParent(parent);
		
		//Job getLogs = this.getOxoGLogs(runOxoGWorkflow);

		return runOxoGWorkflow;
	}
	
	/**
	 * Gets logs from the container named oxog_run
	 * @param parent
	 * @return
	 * 
	 */
	@Deprecated
	private Job getOxoGLogs(Job parent) {
		//TODO: Either update this to make it more relevant or remove it.
		Job getLog = this.getWorkflow().createBashJob("get OxoG docker logs");
		// This will get the docker logs and print them to stdout, but we may also want to get the logs
		// in the mounted oxog_workspace dir...
		getLog.setCommand("sudo docker logs oxog_run");
		getLog.addParent(parent);
		return getLog;
	}

	/**
	 * Uploads files... TBC...
	 * @param parentJob
	 * @return
	 */
	private Job doUpload(Job parentJob) {
		// Will need to run gtupload to generate the analysis.xml and manifest files (but not actually upload). 
		// The tar file contains all results.
		Job generateAnalysisFiles = this.getWorkflow().createBashJob("generate_analysis_files_for_upload");
		
		//Files to upload:
		//OxoG files
		//minibams
		//other intermediate files?
		
		//first thing to do is generate MD5 sums for all uploadable files.
		for (String file : this.filesToUpload)
		{
			//md5sum test_files/tumour_minibam.bam.bai | cut -d ' ' -f 1 > test_files/tumour_minibam.bai.md5
			generateAnalysisFiles.getCommand().addArgument(" md5sum "+file+" | cut -d ' ' -f 1 > "+file+".md5 \n");
		}
		generateAnalysisFiles.addParent(parentJob);
		
		//Note: It was decided there should be two uploads: one for minibams and one for VCFs (for people who want VCFs but not minibams).

		Job uploadResults = this.getWorkflow().createBashJob("upload results");
		uploadResults.setCommand("rsync /cga/fh/pcawg_pipeline/jobResults_pipette/results/" + this.aliquotID
				+ ".oxoG.somatic.snv_mnv.vcf.gz.tar  " + this.uploadURL);
		uploadResults.addParent(generateAnalysisFiles);
		return uploadResults;
	}
	


	/**
	 * Build the workflow!!
	 */
	@Override
	public void buildWorkflow() {
		try {
			this.init();
			// Pull the repo.
			Job pullRepo = GitUtils.pullRepo(this.getWorkflow(), this.GITPemFile, this.GITname, this.JSONrepo, this.JSONrepoName, this.JSONlocation, this.GITemail  );
			
			Job copy = this.copyCredentials(pullRepo);
			
			// indicate job is in downloading stage.
			Job move2download = GitUtils.gitMove("queued-jobs", "downloading-jobs", this.getWorkflow(), this.JSONlocation, this.JSONrepoName, this.JSONfolderName, this.GITname, this.GITemail, this.gitMoveTestMode, this.JSONfileName ,copy);

			//Download jobs. VCFs downloading serial. Trying to download all in parallel seems to put too great a strain on the system 
			//since the icgc-storage-client can make full use of all cores on a multi-core system. 
			Job downloadSangerVCFs = this.getVCF(move2download, Pipeline.sanger, this.sangerSNVVCFObjectID, this.sangerSNVIndexObjectID,
											this.sangerSVVCFObjectID, this.sangerSVIndexObjectID,
											this.sangerIndelVCFObjectID, this.sangerIndelIndexObjectID);
			Job downloadDkfzEmblVCFs = this.getVCF(downloadSangerVCFs, Pipeline.dkfz_embl, this.dkfzemblSNVVCFObjectID, this.dkfzemblSNVIndexObjectID,
										this.dkfzemblSVVCFObjectID, this.dkfzemblSVIndexObjectID,
										this.dkfzemblIndelVCFObjectID, this.dkfzemblIndelIndexObjectID);
			Job downloadBroadVCFs = this.getVCF(downloadDkfzEmblVCFs, Pipeline.broad, this.broadSNVVCFObjectID, this.broadSNVIndexObjectID,
										this.broadSVVCFObjectID, this.broadSVIndexObjectID,
										this.broadIndelVCFObjectID, this.broadIndelIndexObjectID);
			Job downloadMuseVCFs = this.getVCF(downloadBroadVCFs, Pipeline.muse, this.museSNVVCFObjectID, this.museSNVIndexObjectID);
			// Once VCFs are downloaded, download the BAMs.
			Job downloadNormalBam = this.getBAM(downloadMuseVCFs,BAMType.normal, this.bamNormalIndexObjectID,this.bamNormalObjectID);
			//this.normalBAMFileName = "/datastore/bam/normal/*.bam";
			Job downloadTumourBam = this.getBAM(downloadNormalBam,BAMType.tumour, this.bamTumourIndexObjectID,this.bamTumourObjectID);
			//this.tumourBAMFileName = "/datastore/bam/tumour/*.bam";
			
			// After we've downloaded all VCFs on a per-workflow basis, we also need to do a vcfcombine 
			// on the *types* of VCFs, for the minibam generator. The per-workflow combined VCFs will
			// be used by the OxoG filter. These three can be done in parallel because they all require the same inputs, 
			// but none require the inputs of the other and they are not very intense jobs.
			// indicate job is running.
			Job move2running = GitUtils.gitMove( "downloading-jobs", "running-jobs", this.getWorkflow(),
					this.JSONlocation, this.JSONrepoName, this.JSONfolderName, this.GITname, this.GITemail, this.gitMoveTestMode, this.JSONfileName
					, downloadSangerVCFs, downloadDkfzEmblVCFs, downloadBroadVCFs, downloadMuseVCFs, downloadNormalBam, downloadTumourBam);

			// OxoG will run after move2running. Move2running will run after all the jobs that perform input file downloads and file preprocessing have finished.  
			Job sangerPreprocessVCF = this.preProcessVCF(move2running, Pipeline.sanger, this.sangerIndelName);
			Job dkfzEmblPreprocessVCF = this.preProcessVCF(move2running, Pipeline.dkfz_embl, this.dkfzEmblIndelName);
			Job broadPreprocessVCF = this.preProcessVCF(move2running, Pipeline.broad, this.broadIndelName);
			
			Job combineVCFsByType = this.combineVCFsByType( sangerPreprocessVCF, dkfzEmblPreprocessVCF, broadPreprocessVCF);
			
			Job oxoG = this.doOxoG(combineVCFsByType);
			Job oxoGSnvsFromIndels = this.doOxoGSnvsFromIndels(oxoG);
			// variantbam jobs will run parallel to oxog. variant seems to only use a *single* core, but runs long ( 60 - 120 min on OpenStack);
			// OxoG uses a few cores (sometimes), but runs shorter (~20 min, on OpenStack).
			Job normalVariantBam = this.doVariantBam(move2running,BAMType.normal,"/datastore/bam/"+this.bamNormalObjectID+"/"+this.normalBAMFileName);
			Job tumourVariantBam = this.doVariantBam(move2running,BAMType.tumour,"/datastore/bam/"+this.bamTumourObjectID+"/"+this.tumourBAMFileName);
	
			// indicate job is in uploading stage.
			Job move2uploading = GitUtils.gitMove("running-jobs", "uploading-jobs", this.getWorkflow(), this.JSONlocation, this.JSONrepoName, this.JSONfolderName, this.GITname, this.GITemail, this.gitMoveTestMode, this.JSONfileName , oxoGSnvsFromIndels, normalVariantBam, tumourVariantBam);
			
			//Now do the Upload
			Job uploadResults = doUpload(move2uploading);
	
			// indicate job is complete.
			/*Job move2finished = */GitUtils.gitMove( "uploading-jobs", "completed-jobs", this.getWorkflow(), this.JSONlocation, this.JSONrepoName, this.JSONfolderName, this.GITname, this.GITemail, this.gitMoveTestMode, this.JSONfileName , uploadResults);
		}
		catch (Exception e)
		{
			throw new RuntimeException ("Exception caught: "+e.getMessage(), e);
		}
	}
}
