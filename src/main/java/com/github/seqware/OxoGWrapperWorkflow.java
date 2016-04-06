package com.github.seqware;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import com.github.seqware.downloaders.DownloaderBuilder;
import com.github.seqware.downloaders.GNOSDownloader;
import com.github.seqware.downloaders.ICGCStorageDownloader;
import com.github.seqware.downloaders.S3Downloader;

import net.sourceforge.seqware.pipeline.workflowV2.model.Job;



public class OxoGWrapperWorkflow extends BaseOxoGWrapperWorkflow {

	private Collector<String[], ?, Map<String, Object>> collectToMap = Collectors.toMap(kv -> kv[0], kv -> kv[1]);;

	public enum DownloadMethod
	{
		gtdownload, icgcStorageClient, s3
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
		Job copy = this.getWorkflow().createBashJob("copy /home/ubuntu/.gnos");
		copy.setCommand("mkdir /datastore/credentials && cp -r /home/ubuntu/.gnos/* /datastore/credentials && ls -l /datastore/credentials");
		copy.addParent(parentJob);
		
		if (this.downloadMethod.equals(DownloadMethod.s3.toString()))
		{
			Job s3Setup = this.getWorkflow().createBashJob("s3 credentials setup");
			s3Setup.setCommand("mkdir ~/.aws && cp /datastore/credentials/aws_credentials ~/.aws/credentials");
			s3Setup.addParent(copy);
			return s3Setup;
		}
		else
		{
			return copy;
		}
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
	private Job getBAM(Job parentJob, DownloadMethod downloadMethod, BAMType bamType, String ... objectIDs) {
		Job getBamFileJob = this.getWorkflow().createBashJob("get "+bamType.toString()+" BAM file");
		getBamFileJob.addParent(parentJob);
		
		String outDir = "/datastore/bam/"+bamType.toString()+"/";
		String getBamCommandString;
		switch (downloadMethod)
		{
			case icgcStorageClient:
				getBamCommandString = (DownloaderBuilder.of(ICGCStorageDownloader::new).with(ICGCStorageDownloader::setStorageSource, this.storageSource).build()).getDownloadCommandString(outDir, bamType.toString(), objectIDs);
				break;
			case gtdownload:
				getBamCommandString = (DownloaderBuilder.of(GNOSDownloader::new).with(GNOSDownloader::setDownloadKey, this.gtDownloadBamKey).build()).getDownloadCommandString(outDir, bamType.toString(), objectIDs);
				break;
			case s3:
				getBamCommandString = (DownloaderBuilder.of(S3Downloader::new).build()).getDownloadCommandString(outDir, bamType.toString(), objectIDs);
				break;
			default:
				throw new RuntimeException("Unknown downloadMethod: "+downloadMethod.toString());
		}
		
		String moveToFailed = GitUtils.gitMoveCommand("downloading-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, this.getWorkflowBaseDir() + "/scripts/");
		getBamFileJob.setCommand("( "+getBamCommandString+" ) || "+moveToFailed);

		return getBamFileJob;
	}

	private Job getBAM(Job parentJob, DownloadMethod downloadMethod, BAMType bamType, List<String> objectIDs) {
		return getBAM(parentJob, downloadMethod, bamType, objectIDs.toArray(new String[objectIDs.size()]));
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
	private Job getVCF(Job parentJob, DownloadMethod downloadMethod, Pipeline workflowName, String ... objectIDs) {
		Job getVCFJob = this.getWorkflow().createBashJob("get VCF for workflow " + workflowName);
		String outDir = "/datastore/vcf/"+workflowName;
		String moveToFailed = GitUtils.gitMoveCommand("downloading-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, this.getWorkflowBaseDir() + "/scripts/");
		String getVCFCommand ;
		switch (downloadMethod)
		{
			case icgcStorageClient:
				//System.out.println("DEBUG: storageSource: "+this.storageSource);
				getVCFCommand = ( DownloaderBuilder.of(ICGCStorageDownloader::new).with(ICGCStorageDownloader::setStorageSource, this.storageSource).build() ).getDownloadCommandString(outDir, workflowName.toString(), objectIDs);
				break;
			case gtdownload:
				//System.out.println("DEBUG: gtDownloadKey: "+this.gtDownloadVcfKey);
				getVCFCommand = ( DownloaderBuilder.of(GNOSDownloader::new).with(GNOSDownloader::setDownloadKey, this.gtDownloadVcfKey).build() ).getDownloadCommandString(outDir, workflowName.toString(), objectIDs);
				break;
			case s3:
				getVCFCommand = ( DownloaderBuilder.of(S3Downloader::new).build() ).getDownloadCommandString(outDir, workflowName.toString(), objectIDs);;
				break;
			default:
				throw new RuntimeException("Unknown downloadMethod: "+downloadMethod.toString());
		}
		getVCFCommand += (" || " + moveToFailed);
		getVCFJob.setCommand(getVCFCommand);
		
		getVCFJob.addParent(parentJob);

		return getVCFJob;
	}

	private Job getVCF(Job parentJob, DownloadMethod downloadMethod, Pipeline workflowName, List<String> objectIDs) {
		return this.getVCF(parentJob, downloadMethod, workflowName, objectIDs.toArray(new String[objectIDs.size()]));
	}
	
	
	/**
	 * Perform filtering on all VCF files for a given workflow.
	 * Filtering involves removing lines that are not "PASS" or "."
	 * Output files will have ".pass-filtered." in their name.
	 * @param workflowName The workflow to PASS filter
	 * @param parents List of parent jobs.
	 * @return
	 */
	private Job passFilterWorkflow(Pipeline workflowName, Job ... parents)
	{
		Job passFilter = this.getWorkflow().createBashJob("pass filter "+workflowName);
		String moveToFailed = GitUtils.gitMoveCommand("running-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, this.getWorkflowBaseDir() + "/scripts/");
		//If we were processing MUSE files we would also need to filter for tier* but we're NOT processing MUSE files so 
		//we don't need to worry about that for now.
		Map<String,Object> context = new HashMap<String,Object>(2);
		context.put("workflowName", workflowName.toString());
		context.put("moveToFailed", moveToFailed);
		String renderedTemplate;
		
		renderedTemplate = TemplateUtils.getRenderedTemplate(context, "passFilter.template");
		
		passFilter.setCommand(renderedTemplate);
		
		for (Job parent : parents)
		{
			passFilter.addParent(parent);
		}
		
		return passFilter;
	}

	/*
	 * Yes, install tabix as a part of the workflow. It's not in the seqware_whitestar or seqware_whitestar_pancancer container, so
	 * install it here.
	 */
	private Job installTools(Job parent)
	{
		Job installTabixJob = this.getWorkflow().createBashJob("install tools: tabix and bgzip");
		
		installTabixJob.setCommand("sudo apt-get install tabix ");
		installTabixJob.addParent(parent);
		return installTabixJob;
	}
	
	/**
	 * Pre-processes INDEL VCFs. Normalizes INDELs and extracts SNVs from normalized INDELs.
	 * @param parent
	 * @param workflowName The name of the workflow whose files will be pre-processed.
	 * @param vcfName The name of the INDEL VCF to normalize.
	 * @return
	 */
	private Job preProcessIndelVCF(Job parent, Pipeline workflowName, String vcfName )
	{
		String outDir = "/datastore/vcf/"+workflowName;
		String normalizedINDELName = this.aliquotID+ "_"+ workflowName+"_somatic.indel.pass-filtered.bcftools-norm.vcf.gz";
		String extractedSNVVCFName = this.aliquotID+ "_"+ workflowName+"_somatic.indel.pass-filtered.bcftools-norm.extracted-snvs.vcf";
		String fixedIndel = vcfName.replace("indel.", "indel.fixed.").replace(".gz", ""); //...because the fixed indel will not be a gz file - at least not immediately.
		Job bcfToolsNormJob = this.getWorkflow().createBashJob("normalize "+workflowName+" Indels");
		String sedTab = "\\\"$(echo -e '\\t')\\\"";
		String runBCFToolsNormCommand = TemplateUtils.getRenderedTemplate(Arrays.stream(new String[][]{
			{ "sedTab", sedTab }, { "outDir", outDir }, { "vcfName", vcfName }, { "workflowName", workflowName.toString() } ,
			{ "refFile", this.refFile }, { "fixedIndel", fixedIndel }, { "normalizedINDELName", normalizedINDELName } 
		}).collect(this.collectToMap), "bcfTools.template");

		String moveToFailed = GitUtils.gitMoveCommand("running-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, this.getWorkflowBaseDir() + "/scripts/");				 
		runBCFToolsNormCommand += (" || " + moveToFailed );
		
		bcfToolsNormJob.setCommand(runBCFToolsNormCommand);
		bcfToolsNormJob.addParent(parent);
		
		//Normalized INDELs should be indexed uploaded
		
		filesForUpload.add(outDir+"/"+normalizedINDELName);
		filesForUpload.add(outDir+"/"+normalizedINDELName+".tbi");
		
		Job extractSNVFromIndel = this.getWorkflow().createBashJob("extracting SNVs from "+workflowName+" INDEL");
		String extractSNVFromIndelCommand = "( bgzip -d -c "+outDir+"/"+normalizedINDELName+" > "+outDir+"/"+normalizedINDELName+"_somatic.indel.bcftools-norm.vcf \\\n"
											+ " && grep -e '^#' -i -e '^[^#].*[[:space:]][ACTG][[:space:]][ACTG][[:space:]]' "+outDir+"/"+normalizedINDELName+"_somatic.indel.bcftools-norm.vcf \\\n"
											+ "> "+outDir+"/"+extractedSNVVCFName
											+ " && bgzip -f "+outDir+"/"+extractedSNVVCFName
											+ " && tabix -f -p vcf "+outDir+"/"+extractedSNVVCFName + ".gz  ) ";
		
		extractSNVFromIndelCommand += (" || " + moveToFailed );
		extractSNVFromIndel.setCommand(extractSNVFromIndelCommand);
		extractSNVFromIndel.addParent(bcfToolsNormJob);
		
		switch (workflowName) {
			case sanger:
				this.sangerNormalizedIndelVCFName = outDir + "/"+normalizedINDELName;
				this.sangerExtractedSNVVCFName = outDir + "/"+extractedSNVVCFName+".gz";
				break;
			case broad:
				this.broadNormalizedIndelVCFName = outDir + "/"+normalizedINDELName;
				this.broadExtractedSNVVCFName = outDir + "/"+extractedSNVVCFName+".gz";
				break;
			case dkfz_embl:
				this.dkfzEmblNormalizedIndelVCFName = outDir + "/"+normalizedINDELName;
				this.dkfzEmblExtractedSNVVCFName = outDir + "/"+extractedSNVVCFName+".gz";
				break;
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
		Job prepVCFs = this.getWorkflow().createBashJob("create links to VCFs");
		String prepCommand = "";
		prepCommand+="\n ( ( [ -d /datastore/merged_vcfs ] || sudo mkdir /datastore/merged_vcfs/ ) && sudo chmod a+rw /datastore/merged_vcfs && \\\n"
		+"\n ln -s /datastore/vcf/"+Pipeline.sanger+"/"+this.sangerGnosID+"/"+this.sangerSNVName+" /datastore/vcf/"+Pipeline.sanger+"_snv.vcf && \\\n"
		+" ln -s /datastore/vcf/"+Pipeline.broad+"/"+this.broadGnosID+"/"+this.broadSNVName+" /datastore/vcf/"+Pipeline.broad+"_snv.vcf && \\\n"
		+" ln -s /datastore/vcf/"+Pipeline.dkfz_embl+"/"+this.dkfzemblGnosID+"/"+this.dkfzEmblSNVName+" /datastore/vcf/"+Pipeline.dkfz_embl+"_snv.vcf && \\\n"
		+" ln -s /datastore/vcf/"+Pipeline.muse+"/"+this.museGnosID+"/"+this.museSNVName+" /datastore/vcf/"+Pipeline.muse+"_snv.vcf && \\\n"
		+" ln -s "+this.sangerNormalizedIndelVCFName+" /datastore/vcf/"+Pipeline.sanger+"_indel.vcf && \\\n"
		+" ln -s "+this.broadNormalizedIndelVCFName+" /datastore/vcf/"+Pipeline.broad+"_indel.vcf && \\\n"
		+" ln -s "+this.dkfzEmblNormalizedIndelVCFName+" /datastore/vcf/"+Pipeline.dkfz_embl+"_indel.vcf && \\\n"
		+" ln -s /datastore/vcf/"+Pipeline.sanger+"/"+this.sangerGnosID+"/"+this.sangerSVName+" /datastore/vcf/"+Pipeline.sanger+"_sv.vcf && \\\n"
		+" ln -s /datastore/vcf/"+Pipeline.broad+"/"+this.broadGnosID+"/"+this.broadSVName+" /datastore/vcf/"+Pipeline.broad+"_sv.vcf && \\\n"
		+" ln -s /datastore/vcf/"+Pipeline.dkfz_embl+"/"+this.dkfzemblGnosID+"/"+this.dkfzEmblSVName+" /datastore/vcf/"+Pipeline.dkfz_embl+"_sv.vcf ) ";
		
		String moveToFailed = GitUtils.gitMoveCommand("running-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, this.getWorkflowBaseDir() + "/scripts/");
		prepCommand += (" || " + moveToFailed);
		
		prepVCFs.setCommand(prepCommand);
		
		for (Job parent : parents)
		{
			prepVCFs.addParent(parent);
		}
		
		Job vcfCombineJob = this.getWorkflow().createBashJob("combining VCFs by type");
		
		//run the merge script, then bgzip and index them all.
		String combineCommand = "( perl "+this.getWorkflowBaseDir()+"/scripts/vcf_merge_by_type.pl "
				+ Pipeline.broad+"_snv.vcf "+Pipeline.sanger+"_snv.vcf "+Pipeline.dkfz_embl+"_snv.vcf "+Pipeline.muse+"_snv.vcf "
				+ Pipeline.broad+"_indel.vcf "+Pipeline.sanger+"_indel.vcf "+Pipeline.dkfz_embl+"_indel.vcf " 
				+ Pipeline.broad+"_sv.vcf "+Pipeline.sanger+"_sv.vcf "+Pipeline.dkfz_embl+"_sv.vcf "
				+ " /datastore/vcf/ /datastore/merged_vcfs/ "
				+ " ) || "+moveToFailed;

		vcfCombineJob.setCommand(combineCommand);
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
	private Job doOxoG(String pathToTumour, String tumourID, Job ...parents) {
		
		String moveToFailed = GitUtils.gitMoveCommand("running-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, this.getWorkflowBaseDir() + "/scripts/");
		Function<String,String> getFileName = (s) -> {  return s.substring(s.lastIndexOf("/")); };
		Job extractOutputFiles = this.getWorkflow().createBashJob("extract oxog output files from tar");
		if (!skipOxoG)
		{
			Job runOxoGWorkflow = this.getWorkflow().createBashJob("run OxoG Filter");
			String runOxoGCommand = TemplateUtils.getRenderedTemplate(Arrays.stream(new String[][] {
					{ "sangerExtractedSNVVCFPath", this.sangerExtractedSNVVCFName }, { "sangerWorkflow", Pipeline.sanger.toString() },
					{ "sangerExtractedSNVVCF", getFileName.apply(this.sangerExtractedSNVVCFName) },
					{ "broadExtractedSNVVCFPath", this.broadExtractedSNVVCFName }, { "broadWorkflow", Pipeline.broad.toString() },
					{ "broadExtractedSNVVCF", getFileName.apply(this.broadExtractedSNVVCFName) },
					{ "dkfzEmblExtractedSNVVCFPath", this.dkfzEmblExtractedSNVVCFName }, { "dkfzEmblWorkflow", Pipeline.dkfz_embl.toString() },
					{ "dkfzEmblExtractedSNVVCF", getFileName.apply(this.dkfzEmblExtractedSNVVCFName) },
					{ "tumourID", tumourID }, { "aliquotID", this.aliquotID }, { "oxoQScore", this.oxoQScore }, { "museWorkflow", Pipeline.muse.toString() },
					{ "pathToTumour", pathToTumour }, { "normalBamGnosID", this.normalBamGnosID }, { "normalBAMFileName", this.normalBAMFileName } ,
					{ "broadGnosID", this.broadGnosID }, { "sangerGnosID", this.sangerGnosID }, { "dkfzemblGnosID", this.dkfzemblGnosID }, { "museGnosID", this.museGnosID },
					{ "sangerSNVName", this.sangerSNVName}, { "broadSNVName", this.broadSNVName }, { "dkfzEmblSNVName", this.dkfzEmblSNVName }, { "museSNVName", this.museSNVName }			
				} ).collect(collectToMap), "runOxoGFilter.template");
			runOxoGWorkflow.setCommand("( "+runOxoGCommand+" ) || "+ moveToFailed);

			for (Job parent : parents)
			{
				runOxoGWorkflow.addParent(parent);
			}
			extractOutputFiles.addParent(runOxoGWorkflow);
		}
		else
		{
			//Assume that OxoG already ran so we can skip ahead to extracting output files!
			for (Job parent : parents)
			{
				extractOutputFiles.addParent(parent);
			}
		}
		extractOutputFiles.setCommand("(cd /datastore/oxog_results/tumour_"+tumourID+"/ && sudo chmod a+rw -R /datastore/oxog_results/ && tar -xvkf ./"+this.aliquotID+".gnos_files.tar  ) || "+moveToFailed);

		String pathToResults = "/datastore/oxog_results/tumour_"+tumourID+"/cga/fh/pcawg_pipeline/jobResults_pipette/jobs/"+this.aliquotID+"/links_for_gnos/annotate_failed_sites_to_vcfs/";
		String pathToUploadDir = "/datastore/files_for_upload/tumour_"+tumourID+"/";
		
		Function<String,String> changeToOxoGSuffix = (s) -> {return pathToUploadDir + s.replace(".vcf.gz", ".oxoG.vcf.gz"); };
		Function<String,String> changeToOxoGTBISuffix = changeToOxoGSuffix.andThen((s) -> s+=".tbi"); 
		//regular VCFs
		this.filesForUpload.add(changeToOxoGSuffix.apply(this.broadSNVName));
		this.filesForUpload.add(changeToOxoGSuffix.apply(this.dkfzEmblSNVName));
		this.filesForUpload.add(changeToOxoGSuffix.apply(this.sangerSNVName));
		this.filesForUpload.add(changeToOxoGSuffix.apply(this.museSNVName));
		//index files
		this.filesForUpload.add(changeToOxoGTBISuffix.apply(this.broadSNVName));
		this.filesForUpload.add(changeToOxoGTBISuffix.apply(this.dkfzEmblSNVName));
		this.filesForUpload.add(changeToOxoGTBISuffix.apply(this.sangerSNVName));
		this.filesForUpload.add(changeToOxoGTBISuffix.apply(this.museSNVName));
		//Extracted SNVs
		this.filesForUpload.add(changeToOxoGSuffix.apply(getFileName.apply(this.sangerExtractedSNVVCFName)));
		this.filesForUpload.add(changeToOxoGSuffix.apply(getFileName.apply(this.broadExtractedSNVVCFName)));
		this.filesForUpload.add(changeToOxoGSuffix.apply(getFileName.apply(this.dkfzEmblExtractedSNVVCFName)));
		//index files
		this.filesForUpload.add(changeToOxoGTBISuffix.apply(getFileName.apply(this.sangerExtractedSNVVCFName)));
		this.filesForUpload.add(changeToOxoGTBISuffix.apply(getFileName.apply(this.broadExtractedSNVVCFName)));
		this.filesForUpload.add(changeToOxoGTBISuffix.apply(getFileName.apply(this.dkfzEmblExtractedSNVVCFName)));
		
		this.filesForUpload.add("/datastore/oxog_results/" + this.aliquotID + ".gnos_files.tar");
			
		Job prepOxoGTarAndMutectCallsforUpload = this.getWorkflow().createBashJob("prepare OxoG tar and mutect calls file for upload");
		prepOxoGTarAndMutectCallsforUpload.setCommand("( ([ -d "+pathToUploadDir+" ] || mkdir -p "+pathToUploadDir+") "
				+ " && cp /datastore/oxog_results/tumour_"+tumourID+"/"+this.aliquotID+".gnos_files.tar "+pathToUploadDir+" \\\n"
				+ " && cp " + pathToResults + "*.vcf.gz  "+pathToUploadDir+" \\\n"
				+ " && cp " + pathToResults + "*.vcf.gz.tbi  "+pathToUploadDir+" \\\n"
				// Also need to upload normalized INDELs - TODO: Move to its own job, or maybe combine with the normalization job?
				+ " && cp " + this.broadNormalizedIndelVCFName+" "+pathToUploadDir+" \\\n"
				+ " && cp " + this.dkfzEmblNormalizedIndelVCFName+" "+pathToUploadDir+" \\\n"
				+ " && cp " + this.sangerNormalizedIndelVCFName+" "+pathToUploadDir+" \\\n"
				+ " && cp " + this.broadNormalizedIndelVCFName+".tbi "+pathToUploadDir+" \\\n"
				+ " && cp " + this.dkfzEmblNormalizedIndelVCFName+".tbi "+pathToUploadDir+" \\\n"
				+ " && cp " + this.sangerNormalizedIndelVCFName+".tbi "+pathToUploadDir+" \\\n"
				// Copy the call_stats 
				+ " && cp /datastore/oxog_workspace/tumour_"+tumourID+"/mutect/sg/gather/"+this.aliquotID+".call_stats.txt "+pathToUploadDir+this.aliquotID+".call_stats.txt \\\n"
				+ " && cd "+pathToUploadDir+" && gzip -f "+this.aliquotID+".call_stats.txt && tar -cvf ./"+this.aliquotID+".call_stats.txt.gz.tar ./"+this.aliquotID+".call_stats.txt.gz ) || "+moveToFailed);
		this.filesForUpload.add("/datastore/files_for_upload/tumour_"+tumourID+"/"+this.aliquotID+".call_stats.txt.gz.tar");
		
		prepOxoGTarAndMutectCallsforUpload.addParent(extractOutputFiles);
		
		return prepOxoGTarAndMutectCallsforUpload;
		
	}

	/**
	 * Runs the variant program inside the Broad's OxoG container to produce a mini-BAM for a given BAM. 
	 * @param parent
	 * @param bamType - The type of BAM file to use. Determines the name of the output file.
	 * @param bamPath - The path to the input BAM file.
	 * @return
	 */
	private Job doVariantBam(Job parent, BAMType bamType, String bamPath, String tumourBAMFileName, String tumourMinibamPath,String tumourID) {
		Job runOxoGWorkflow = this.getWorkflow().createBashJob("run "+bamType+" variantbam");

		String minibamName = "";
		if (bamType == BAMType.normal)
		{
			minibamName = this.normalBAMFileName.replace(".bam", "_minibam");
			this.normalMinibamPath = "/datastore/variantbam_results/"+minibamName+".bam";
			this.filesForUpload.add(this.normalMinibamPath);
			this.filesForUpload.add(this.normalMinibamPath+".bai");
		}
		else
		{
			minibamName = tumourBAMFileName.replace(".bam", "_minibam");
			tumourMinibamPath = "/datastore/variantbam_results/"+minibamName+".bam";
			
			for (int i = 0; i < this.tumours.size(); i++ )
			{
				if (this.tumours.get(i).getTumourBamGnosID().equals(tumourID))
				{
					this.tumours.get(i).setTumourMinibamPath(tumourMinibamPath);
				}
			}
			
			this.filesForUpload.add(tumourMinibamPath);
			this.filesForUpload.add(tumourMinibamPath+".bai");
		}
		
		if (!this.skipVariantBam)
		{
			String command = DockerCommandCreator.createVariantBamCommand(bamType, minibamName+".bam", bamPath, this.snvVCF, this.svVCF, this.indelVCF, this.svPadding, this.snvPadding, this.indelPadding,tumourID);
			
			command = "(( [ -d /datastore/variantbam_results/ ] || mkdir -p /datastore/variantbam_results ) && sudo chmod a+rw -R /datastore/variantbam_results/ && " + command + " ) ";
			
			String moveToFailed = GitUtils.gitMoveCommand("running-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, this.getWorkflowBaseDir() + "/scripts/");
			command += (" || " + moveToFailed);
			runOxoGWorkflow.setCommand(command);
		}
		runOxoGWorkflow.addParent(parent);
		
		//Job getLogs = this.getOxoGLogs(runOxoGWorkflow);
		return runOxoGWorkflow;
	}
	
	/**
	 * Uploads files. Will use the vcf-upload script in pancancer/pancancer_upload_download:1.7 to generate metadata.xml, analysis.xml, and the GTO file, and
	 * then rsync everything to a staging server. 
	 * @param parentJob
	 * @return
	 */
	private Job doUpload(Job parentJob) {
		String moveToFailed = GitUtils.gitMoveCommand("uploading-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, this.getWorkflowBaseDir() + "/scripts/");
		// Will need to run gtupload to generate the analysis.xml and manifest files (but not actually upload). 
		Job generateAnalysisFilesVCFs = generateVCFMetadata(parentJob, moveToFailed);
		
		Job generateAnalysisFilesBAMs = generateBAMMetadata(parentJob, moveToFailed);
	
		String gnosServer = this.gnosMetadataUploadURL.replace("http://", "").replace("https://", "").replace("/", "");
		//Note: It was decided there should be two uploads: one for minibams and one for VCFs (for people who want VCFs but not minibams).
		Job uploadVCFResults = this.getWorkflow().createBashJob("upload VCF results");
		String uploadVCFCommand = "sudo chmod 0600 /datastore/credentials/rsync.key\n"
								+ "UPLOAD_PATH=$( echo \""+this.uploadURL+"\" | sed 's/\\(.*\\)\\:\\(.*\\)/\\2/g' )\n"
								+ "VCF_UUID=$(grep server_path /datastore/files_for_upload/manifest.xml  | sed 's/.*server_path=\\\"\\(.*\\)\\\" .*/\\1/g')\n"
								+ "( rsync -avtuz -e 'ssh -o UserKnownHostsFile=/datastore/credentials/known_hosts -o IdentitiesOnly=yes -o BatchMode=yes -o PasswordAuthentication=no -o PreferredAuthentications=publickey -i "+this.uploadKey+"'"
										+ " --rsync-path=\"mkdir -p $UPLOAD_PATH/"+gnosServer+"/$VCF_UUID && rsync\" /datastore/files_for_upload/ " + this.uploadURL+ "/"+gnosServer + "/$VCF_UUID ) ";
		uploadVCFCommand += (" || " + moveToFailed);
		uploadVCFResults.setCommand(uploadVCFCommand);
		uploadVCFResults.addParent(generateAnalysisFilesVCFs);
		
		Job uploadBAMResults = this.getWorkflow().createBashJob("upload BAM results");
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

	private Job generateBAMMetadata(Job parentJob, String moveToFailed) {
		Job generateAnalysisFilesBAMs = this.getWorkflow().createBashJob("generate_analysis_files_for_BAM_upload");
		
		String bams = "";
		String bamIndicies = "";
		String bamMD5Sums = "";
		String bamIndexMD5Sums = "";
		String generateAnalysisFilesBAMsCommand = "";
		generateAnalysisFilesBAMsCommand += "sudo chmod a+rw -R /datastore/variantbam_results/ &&\n";
		//I don't think distinct() should be necessary.
		for (String file : this.filesForUpload.stream().filter( p -> p.contains(".bam") || p.contains(".bai") ).distinct().collect(Collectors.toList()) )
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
				{"workflowName", this.getName()}, {"workflowVersion", this.getVersion()}, {"workflowURL", this.workflowURL},
				{"workflowSrcURL", this.workflowSourceURL}, {"changeLogURL", this.changelogURL}, {"descriptionSuffix", descriptionEnd}
			}).collect(this.collectToMap), "analysisBAMDescription.template");
		
		generateAnalysisFilesBAMsCommand += TemplateUtils.getRenderedTemplate(Arrays.stream(new String[][] {
				{ "gnosKey", this.gnosKey }, { "gnosMetadataUploadURL", this.gnosMetadataUploadURL }, { "bamDescription", bamDescription },
				{ "normalMetadataURL", this.normalMetdataURL } , { "tumourMetadataURLs", this.tumours.stream().map(t -> t.getTumourMetdataURL()).reduce("", (a,b)->a+=b+"," ) },
				{ "vcfs", bams }, { "bamIndicies", bamIndicies}, { "bamMD5Sums", bamMD5Sums }, { "bamIndexMD5Sums", bamIndexMD5Sums}, 
				{ "studyRefNameOverride", this.studyRefNameOverride }, { "workflowVersion", this.getVersion() } 
			}).collect(collectToMap),"generateBAMAnalysisMetadata.template");

		generateAnalysisFilesBAMs.setCommand("( "+generateAnalysisFilesBAMsCommand+" ) || "+moveToFailed);
		generateAnalysisFilesBAMs.addParent(parentJob);
		return generateAnalysisFilesBAMs;
	}

	private Job generateVCFMetadata(Job parentJob, String moveToFailed) {
		Job generateAnalysisFilesVCFs = this.getWorkflow().createBashJob("generate_analysis_files_for_VCF_upload");
		//Files need to be copied to the staging directory
		String vcfs = "";
		String vcfIndicies = "";
		String vcfMD5Sums = "";
		String vcfIndexMD5Sums = "";
		
		String tars = "";
		String tarMD5Sums = "";
		
		String generateAnalysisFilesVCFCommand = "";
		//I don't think "distinct" should be necessary here, but there were weird duplicates popping up in the list.
		for (String file : this.filesForUpload.stream().filter(p -> ((p.contains(".vcf") || p.endsWith(".tar")) && !( p.contains("SNVs_from_INDELs") || p.contains("extracted-snv"))) ).distinct().collect(Collectors.toList()) )
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
				{ "OxoQScore",this.oxoQScore },					{ "donorID",this.donorID },					{ "specimenID",this.specimenID },
				{ "workflowName",this.getName() },				{ "workflowVersion",this.getVersion() },	{ "workflowURL",this.workflowURL },
				{ "workflowSrcURL",this.workflowSourceURL },	{ "changeLogURL",this.changelogURL },		{ "descriptionSuffix",descriptionEnd },
			}).collect(collectToMap), "analysisVCFDescription.template");

		generateAnalysisFilesVCFCommand = TemplateUtils.getRenderedTemplate(Arrays.stream(new String[][] {
				{ "gnosKey", this.gnosKey }, { "gnosMetadataUploadURL", this.gnosMetadataUploadURL }, { "vcfDescription", vcfDescription },
				{ "normalMetadataURL", this.normalMetdataURL } , { "tumourMetadataURLs", this.tumours.stream().map(t -> t.getTumourMetdataURL()).reduce("", (a,b)->a+=b+"," ) },
				{ "vcfs", vcfs }, { "tars", tars }, { "tarMD5Sums", tarMD5Sums }, { "vcfIndicies", vcfIndicies}, { "vcfMD5Sums", vcfMD5Sums },
				{ "vcfIndexMD5Sums", vcfIndexMD5Sums}, { "studyRefNameOverride", this.studyRefNameOverride }, { "workflowVersion", this.getVersion() } 
			}).collect(collectToMap),"generateVCFAnalysisMetadata.template");
		generateAnalysisFilesVCFs.setCommand("( "+generateAnalysisFilesVCFCommand+ " ) || "+moveToFailed);
		generateAnalysisFilesVCFs.addParent(parentJob);
		return generateAnalysisFilesVCFs;
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
	private Job runAnnotator(String inputType, Pipeline workflowName, String vcfPath, String tumourBamPath, String normalBamPath, String tumourGnosID, Job ...parents)
	{
		String outDir = "/datastore/files_for_upload/tumour_"+tumourGnosID+"/";
		String containerName = "pcawg-annotator_"+workflowName+"_"+inputType;
		String commandName ="run annotator for "+workflowName+" "+inputType;
		String annotatedFileName = this.aliquotID+"_annotated_"+workflowName+"_"+inputType+"_tumour_"+tumourGnosID+".vcf";
		//If a filepath contains the phrase "extracted" then it contains SNVs that were extracted from an INDEL.
		if (vcfPath.contains("extracted"))
		{
			containerName += "_SNVs-from-INDELs";
			commandName += "_SNVs-from-INDELs";
			annotatedFileName = annotatedFileName.replace(inputType, "SNVs_from_INDELs");
		}
		String command = "";
		
		
		Job annotatorJob = this.getWorkflow().createBashJob(commandName);
		if (!this.skipAnnotation)
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
			
			
			String moveToFailed = GitUtils.gitMoveCommand("running-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, this.getWorkflowBaseDir() + "/scripts/");
			command += " || " + moveToFailed;
		}
		this.filesForUpload.add("/datastore/files_for_upload/"+annotatedFileName+".gz ");
		this.filesForUpload.add("/datastore/files_for_upload/"+annotatedFileName+".gz.tbi ");
		
		annotatorJob.setCommand(command);
		for (Job parent : parents)
		{
			annotatorJob.addParent(parent);
		}
		return annotatorJob;
	}

	/*
	 * Wrapper function to GitUtils.giveMove 
	 */
	private Job gitMove(String src, String dest, Job ...parents) throws Exception
	{
		String pathToScripts = this.getWorkflowBaseDir() + "/scripts";
		return GitUtils.gitMove(src, dest, this.getWorkflow(), this.JSONlocation, this.JSONrepoName, this.JSONfolderName, this.GITname, this.GITemail, this.gitMoveTestMode, this.JSONfileName, pathToScripts , (parents));
	}
	
	/**
	 * Does all annotations for the workflow.
	 * @param parents
	 * @return
	 */
	private List<Job> doAnnotations(Job ... parents)
	{
		List<Job> finalAnnotatorJobs = new ArrayList<Job>(3);
		
		Predicate<String> isExtractedSNV = p -> p.contains("extracted-snv") && p.endsWith(".vcf.gz");
		final String passFilteredOxoGSuffix = "somatic.snv_mnv.pass-filtered.oxoG.vcf.gz";
		//list filtering should only ever produce one result.
		String broadOxogSNVFileName = this.filesForUpload.stream().filter(p -> ((p.contains("broad-mutect") && p.endsWith(passFilteredOxoGSuffix)))).collect(Collectors.toList()).get(0);
		String broadOxoGSNVFromIndelFileName = this.filesForUpload.stream().filter(p -> (p.contains(Pipeline.broad.toString()) && isExtractedSNV.test(p) )).collect(Collectors.toList()).get(0);
		
		String sangerOxogSNVFileName = this.filesForUpload.stream().filter(p -> ((p.contains("svcp_") && p.endsWith(passFilteredOxoGSuffix)))).collect(Collectors.toList()).get(0);
		String sangerOxoGSNVFromIndelFileName = this.filesForUpload.stream().filter(p -> (p.contains(Pipeline.sanger.toString()) && isExtractedSNV.test(p) )).collect(Collectors.toList()).get(0);
		
		String dkfzEmbleOxogSNVFileName = this.filesForUpload.stream().filter(p -> ((p.contains("dkfz-snvCalling") && p.endsWith(passFilteredOxoGSuffix)))).collect(Collectors.toList()).get(0);
		String dkfzEmblOxoGSNVFromIndelFileName = this.filesForUpload.stream().filter(p -> (p.contains(Pipeline.dkfz_embl.toString()) && isExtractedSNV.test(p) )).collect(Collectors.toList()).get(0);

		//Remember: MUSE files do not get PASS-filtered. Also, there is no INDEL so there cannot be any SNVs extracted from INDELs.
		String museOxogSNVFileName = this.filesForUpload.stream().filter(p -> p.contains("MUSE") && p.endsWith("somatic.snv_mnv.oxoG.vcf.gz")).collect(Collectors.toList()).get(0);
		
		for (int i = 0; i < this.tumours.size(); i++)
		{
			TumourInfo tInf = this.tumours.get(i);
			Job broadIndelAnnotatorJob = this.runAnnotator("indel", Pipeline.broad, this.broadNormalizedIndelVCFName, tInf.getTumourMinibamPath(),this.normalMinibamPath, tInf.getTumourBamGnosID(), parents);
			Job dfkzEmblIndelAnnotatorJob = this.runAnnotator("indel", Pipeline.dkfz_embl, this.dkfzEmblNormalizedIndelVCFName, tInf.getTumourMinibamPath(), this.normalMinibamPath, tInf.getTumourBamGnosID(), broadIndelAnnotatorJob);
			Job sangerIndelAnnotatorJob = this.runAnnotator("indel", Pipeline.sanger, this.sangerNormalizedIndelVCFName, tInf.getTumourMinibamPath(), this.normalMinibamPath, tInf.getTumourBamGnosID(), dfkzEmblIndelAnnotatorJob);
	
			Job broadSNVAnnotatorJob = this.runAnnotator("SNV", Pipeline.broad,broadOxogSNVFileName, tInf.getTumourMinibamPath(), this.normalMinibamPath, tInf.getTumourBamGnosID(), parents);
			Job dfkzEmblSNVAnnotatorJob = this.runAnnotator("SNV", Pipeline.dkfz_embl,dkfzEmbleOxogSNVFileName, tInf.getTumourMinibamPath(), this.normalMinibamPath, tInf.getTumourBamGnosID(), broadSNVAnnotatorJob);
			Job sangerSNVAnnotatorJob = this.runAnnotator("SNV",Pipeline.sanger,sangerOxogSNVFileName, tInf.getTumourMinibamPath(), this.normalMinibamPath, tInf.getTumourBamGnosID(), dfkzEmblSNVAnnotatorJob);
			Job museSNVAnnotatorJob = this.runAnnotator("SNV",Pipeline.muse,museOxogSNVFileName, tInf.getTumourMinibamPath(), this.normalMinibamPath, tInf.getTumourBamGnosID(), dfkzEmblSNVAnnotatorJob);
	
			Job broadSNVFromIndelAnnotatorJob = this.runAnnotator("SNV",Pipeline.broad, broadOxoGSNVFromIndelFileName, tInf.getTumourMinibamPath(), this.normalMinibamPath, tInf.getTumourBamGnosID(), parents);
			Job dfkzEmblSNVFromIndelAnnotatorJob = this.runAnnotator("SNV",Pipeline.dkfz_embl, dkfzEmblOxoGSNVFromIndelFileName, tInf.getTumourMinibamPath(), this.normalMinibamPath, tInf.getTumourBamGnosID(), broadSNVFromIndelAnnotatorJob);
			Job sangerSNVFromIndelAnnotatorJob = this.runAnnotator("SNV",Pipeline.sanger, sangerOxoGSNVFromIndelFileName, tInf.getTumourMinibamPath(), this.normalMinibamPath, tInf.getTumourBamGnosID(), dfkzEmblSNVFromIndelAnnotatorJob);

			finalAnnotatorJobs.add(sangerSNVFromIndelAnnotatorJob);
			finalAnnotatorJobs.add(sangerSNVAnnotatorJob);
			finalAnnotatorJobs.add(sangerIndelAnnotatorJob);
			finalAnnotatorJobs.add(museSNVAnnotatorJob);
		}
		return finalAnnotatorJobs;
	}

	private Job statInputFiles(Job parent) {
		Job statFiles = this.getWorkflow().createBashJob("stat downloaded input files");
		String moveToFailed = GitUtils.gitMoveCommand("running-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, this.getWorkflowBaseDir() + "/scripts/");
		String statFilesCMD = "( ";
		
		for (String s : new ArrayList<String>( Arrays.asList(this.sangerSNVName,this.sangerSNVIndexFileName,this.sangerSVName,this.sangerSVIndexFileName,this.sangerIndelName,this.sangerINDELIndexFileName) ) ) {
			statFilesCMD+="stat /datastore/vcf/"+Pipeline.sanger.toString()+"/"+this.sangerGnosID+"/"+s+ " && \\\n";
		}

		for (String s : new ArrayList<String>( Arrays.asList(this.broadSNVName,this.broadSNVIndexFileName,this.broadSVName,this.broadSVIndexFileName,this.broadIndelName,this.broadINDELIndexFileName) ) ) {
			statFilesCMD+="stat /datastore/vcf/"+Pipeline.broad.toString()+"/"+this.broadGnosID+"/"+s + " && \\\n";
		}

		for (String s : new ArrayList<String>( Arrays.asList(this.dkfzEmblSNVName,this.dkfzEmblSNVIndexFileName,this.dkfzEmblSVName,this.dkfzEmblSVIndexFileName,this.dkfzEmblIndelName,this.dkfzEmblINDELIndexFileName) ) ) {
			statFilesCMD+="stat /datastore/vcf/"+Pipeline.dkfz_embl.toString()+"/"+this.dkfzemblGnosID+"/"+s + " && \\\n";
		}
		
		statFilesCMD += "stat /datastore/vcf/"+Pipeline.muse.toString()+"/"+this.museGnosID+"/"+this.museSNVName + " && \\\n";
		statFilesCMD += "stat /datastore/vcf/"+Pipeline.muse.toString()+"/"+this.museGnosID+"/"+this.museSNVIndexFileName + " && \\\n";

		//stat all tumour BAMS
		for (int i = 0 ; i < this.tumours.size() ; i++)
		{
			statFilesCMD += "stat /datastore/bam/"+BAMType.tumour.toString()+"/"+this.tumours.get(i).getTumourBamGnosID()+"/"+this.tumours.get(i).getTumourBAMFileName() + " && \\\n";
			statFilesCMD += "stat /datastore/bam/"+BAMType.tumour.toString()+"/"+this.tumours.get(i).getTumourBamGnosID()+"/"+this.tumours.get(i).getTumourBamIndexFileName() + " && \\\n";
		}

		statFilesCMD += "stat /datastore/bam/"+BAMType.normal.toString()+"/"+this.normalBamGnosID+"/"+this.normalBAMFileName + " && \\\n";
		statFilesCMD += "stat /datastore/bam/"+BAMType.normal.toString()+"/"+this.normalBamGnosID+"/"+this.normalBamIndexFileName + " \\\n";
		statFilesCMD += " ) || "+ moveToFailed;
		
		statFiles.setCommand(statFilesCMD);
		
		statFiles.addParent(parent);
		return statFiles;
	}
	
	/**
	 * Build the workflow!!
	 */
	@Override
	public void buildWorkflow() {
		try {
			this.init();

			// Pull the repo.
			Job configJob = GitUtils.gitConfig(this.getWorkflow(), this.GITname, this.GITemail);
			
			Job copy = this.copyCredentials(configJob);
			
			Job pullRepo = GitUtils.pullRepo(this.getWorkflow(), this.GITPemFile, this.JSONrepo, this.JSONrepoName, this.JSONlocation);
			pullRepo.addParent(copy);
			
			Job installTabix = this.installTools(pullRepo);
			
			// indicate job is in downloading stage.
			String pathToScripts = this.getWorkflowBaseDir() + "/scripts";
			Job move2download = GitUtils.gitMove("queued-jobs", "downloading-jobs", this.getWorkflow(), this.JSONlocation, this.JSONrepoName, this.JSONfolderName, this.GITname, this.GITemail, this.gitMoveTestMode, this.JSONfileName, pathToScripts ,installTabix);
			Job move2running;
			if (!skipDownload) {
				//Download jobs. VCFs downloading serial. Trying to download all in parallel seems to put too great a strain on the system 
				//since the icgc-storage-client can make full use of all cores on a multi-core system. 
				DownloadMethod downloadMethod = DownloadMethod.valueOf(this.downloadMethod);
				
				List<String> sangerList = Arrays.asList(this.sangerSNVVCFObjectID, this.sangerSNVIndexObjectID, this.sangerSVVCFObjectID, this.sangerSVIndexObjectID, this.sangerIndelVCFObjectID, this.sangerIndelIndexObjectID);
				List<String> broadList = Arrays.asList(this.broadSNVVCFObjectID, this.broadSNVIndexObjectID, this.broadSVVCFObjectID, this.broadSVIndexObjectID, this.broadIndelVCFObjectID, this.broadIndelIndexObjectID);
				List<String> dkfzEmblList = Arrays.asList(this.dkfzemblSNVVCFObjectID, this.dkfzemblSNVIndexObjectID, this.dkfzemblSVVCFObjectID, this.dkfzemblSVIndexObjectID,this.dkfzemblIndelVCFObjectID, this.dkfzemblIndelIndexObjectID);
				List<String> museList = Arrays.asList(this.museSNVVCFObjectID, this.museSNVIndexObjectID);
				List<String> normalList = Arrays.asList( this.bamNormalIndexObjectID,this.bamNormalObjectID);
				//List<String> tumourList = Arrays.asList( this.bamTumourIndexObjectID,this.bamTumourObjectID);
				//List<List<String>> tumourList = new ArrayList<List<String>>();
				
				Map<String,List<String>> workflowObjectIDs = new HashMap<String,List<String>>(6);
				workflowObjectIDs.put(Pipeline.broad.toString(), broadList);
				workflowObjectIDs.put(Pipeline.sanger.toString(), sangerList);
				workflowObjectIDs.put(Pipeline.dkfz_embl.toString(), dkfzEmblList);
				workflowObjectIDs.put(Pipeline.muse.toString(), museList);
				workflowObjectIDs.put(BAMType.normal.toString(), normalList);
				//workflowObjectIDs.put(BAMType.tumour.toString(), tumourList);
				for (int i = 0; i < this.tumours.size() ; i ++)
				{
					TumourInfo tInfo = this.tumours.get(i);
					List<String> tumourIDs = new ArrayList<String>();
					tumourIDs.add(tInfo.getBamTumourIndexObjectID());
					tumourIDs.add(tInfo.getBamTumourObjectID());
					workflowObjectIDs.put(BAMType.tumour+"_"+tInfo.getTumourBamGnosID(), tumourIDs);
					//tumourList.add(tumourIDs);
				}
				
				Map<String,String> workflowURLs = new HashMap<String,String>(6);
				workflowURLs.put(Pipeline.broad.toString(), this.broadGNOSRepoURL);
				workflowURLs.put(Pipeline.sanger.toString(), this.sangerGNOSRepoURL);
				workflowURLs.put(Pipeline.dkfz_embl.toString(), this.dkfzEmblGNOSRepoURL);
				workflowURLs.put(Pipeline.muse.toString(), this.museGNOSRepoURL);
				workflowURLs.put(BAMType.normal.toString(), this.normalBamGNOSRepoURL);
				for (int i =0 ; i < this.tumours.size(); i++)
				{
					workflowURLs.put(BAMType.tumour.toString()+"_"+i, tumours.get(i).getTumourBamGNOSRepoURL());
				}
				
				Function<String,List<String>> chooseObjects = (s) -> 
				{
					switch (downloadMethod)
					{
						case icgcStorageClient:
							// ICGC storage client - get list of object IDs, use s (workflowname) as lookup.
							return workflowObjectIDs.get(s);
						case s3:
							//For S3 downloader, it will take a list of strings. The strings are of the pattern: <object_id>:<file_name> and it will download all object IDs to the paired filename.
							//We prepend the GNOS ID to the filename because other processes have an expectation (from icgc-storage-client and gtdownload) the files will be in a 
							//directory named with the GNOS ID.
							List<String> objects = workflowObjectIDs.get(s);
							List<String> s3Mappings = objects.stream().map(t -> t + ":" + this.workflowNamestoGnosIds.get(s) + "/" + this.objectToFilenames.get(t) ).collect(Collectors.toList());
							//List<String> s3Mappings = objectIDs.stream().map(s ->  s+":"+this.workflowNamestoGnosIds.get(workflowName)+"/"+this.objectToFilenames.get(s)).collect(Collectors.toList() );
							return s3Mappings;
						case gtdownload:
							// gtdownloader - look up the GNOS URL, return as list with single item. 
							return Arrays.asList(workflowURLs.get(s));
						default:
							throw new RuntimeException("Unknown download method: "+downloadMethod);
					}
				};
				
				Job downloadSangerVCFs = this.getVCF(move2download, downloadMethod, Pipeline.sanger, chooseObjects.apply( Pipeline.sanger.toString() ) );
				Job downloadDkfzEmblVCFs = this.getVCF(downloadSangerVCFs, downloadMethod, Pipeline.dkfz_embl, chooseObjects.apply( Pipeline.dkfz_embl.toString() ) );
				Job downloadBroadVCFs = this.getVCF(downloadDkfzEmblVCFs, downloadMethod, Pipeline.broad, chooseObjects.apply( Pipeline.broad.toString() ) );
				Job downloadMuseVCFs = this.getVCF(downloadBroadVCFs, downloadMethod, Pipeline.muse, chooseObjects.apply( Pipeline.muse.toString() ) );
				// Once VCFs are downloaded, download the BAMs.
				Job downloadNormalBam = this.getBAM(downloadMuseVCFs, downloadMethod, BAMType.normal, chooseObjects.apply( BAMType.normal.toString() ) );
				
				//create a list of jobs to download all tumours.
				List<Job> getTumourJobs = new ArrayList<Job>(this.tumours.size());
				System.out.println("Tumours : "+this.tumours);
				for (int i = 0 ; i < this.tumours.size(); i++)
				{	
					Job downloadTumourBam;
					//download the tumours sequentially.
					if (i==0)
					{
						downloadTumourBam = this.getBAM(downloadNormalBam, downloadMethod, BAMType.tumour,chooseObjects.apply( BAMType.tumour.toString()+"_"+this.tumours.get(i).getTumourBamGnosID() ) );
					}
					else
					{
						downloadTumourBam = this.getBAM(getTumourJobs.get(i-1), downloadMethod, BAMType.tumour,chooseObjects.apply( BAMType.tumour.toString()+"_"+this.tumours.get(i).getTumourBamGnosID() ) );
					}
					getTumourJobs.add(downloadTumourBam);
				}
				
				// After we've downloaded all VCFs on a per-workflow basis, we also need to do a vcfcombine 
				// on the *types* of VCFs, for the minibam generator. The per-workflow combined VCFs will
				// be used by the OxoG filter. These three can be done in parallel because they all require the same inputs, 
				// but none require the inputs of the other and they are not very intense jobs.
				// indicate job is running.
				move2running = GitUtils.gitMove( "downloading-jobs", "running-jobs", this.getWorkflow(),
						this.JSONlocation, this.JSONrepoName, this.JSONfolderName, this.GITname, this.GITemail, this.gitMoveTestMode, this.JSONfileName, pathToScripts
						, downloadSangerVCFs, downloadDkfzEmblVCFs, downloadBroadVCFs, downloadMuseVCFs, downloadNormalBam, getTumourJobs.get(getTumourJobs.size()-1));
			}
			else {
				// If user is skipping download, then we will just move directly to runnning...
				move2running = GitUtils.gitMove("downloading-jobs", "running-jobs", this.getWorkflow(), this.JSONlocation, this.JSONrepoName, this.JSONfolderName, this.GITname, this.GITemail, this.gitMoveTestMode, this.JSONfileName , pathToScripts,move2download);
			}

			Job statFiles = statInputFiles(move2running);
			
			Job sangerPassFilter = this.passFilterWorkflow(Pipeline.sanger, statFiles);
			Job broadPassFilter = this.passFilterWorkflow(Pipeline.broad, statFiles);
			Job dkfzemblPassFilter = this.passFilterWorkflow(Pipeline.dkfz_embl, statFiles);
			// No, we're not going to filter the Muse SNV file.
			

			//update all filenames to include ".pass-filtered."
			Function<String,String> addPassFilteredSuffix = (x) -> { return x.replace(".vcf.gz",".pass-filtered.vcf.gz"); };
			this.sangerSNVName = addPassFilteredSuffix.apply(this.sangerSNVName);
			this.sangerIndelName = addPassFilteredSuffix.apply(this.sangerIndelName);
			this.sangerSVName = addPassFilteredSuffix.apply(this.sangerSVName);

			this.broadSNVName = addPassFilteredSuffix.apply(this.broadSNVName);
			this.broadIndelName = addPassFilteredSuffix.apply(this.broadIndelName);
			this.broadSVName = addPassFilteredSuffix.apply(this.broadSVName);
			
			this.dkfzEmblSNVName = addPassFilteredSuffix.apply(this.dkfzEmblSNVName);
			this.dkfzEmblIndelName = addPassFilteredSuffix.apply(this.dkfzEmblIndelName);
			this.dkfzEmblSVName = addPassFilteredSuffix.apply(this.dkfzEmblSVName);
			
			// OxoG will run after move2running. Move2running will run after all the jobs that perform input file downloads and file preprocessing have finished.  
			Job sangerPreprocessVCF = this.preProcessIndelVCF(sangerPassFilter, Pipeline.sanger,"/"+ this.sangerGnosID +"/"+ this.sangerIndelName);
			Job dkfzEmblPreprocessVCF = this.preProcessIndelVCF(dkfzemblPassFilter, Pipeline.dkfz_embl, "/"+ this.dkfzemblGnosID +"/"+ this.dkfzEmblIndelName);
			Job broadPreprocessVCF = this.preProcessIndelVCF(broadPassFilter, Pipeline.broad, "/"+ this.broadGnosID +"/"+ this.broadIndelName);
			
			Job combineVCFsByType = this.combineVCFsByType( sangerPreprocessVCF, dkfzEmblPreprocessVCF, broadPreprocessVCF);
			
			List<Job> oxogJobs = new ArrayList<Job>(this.tumours.size());
			for (int i = 0 ; i < this.tumours.size(); i++)
			{
				TumourInfo tInf = this.tumours.get(i);
				Job oxoG = this.doOxoG(tInf.getTumourBamGnosID()+"/"+tInf.getTumourBAMFileName(),tInf.getTumourBamGnosID(), combineVCFsByType);
				if (i>0)
				{
					//OxoG jobs can put a heavy CPU load on the system (in bursts) so probably better to run them in sequence.
					//If there is > 1 OxoG job (i.e. a multi-tumour donor), each OxoG job should have the prior OxoG job as its parent. 
					oxoG.addParent(oxogJobs.get(i-1));
				}
				oxogJobs.add(oxoG);
			}
			
			Job normalVariantBam = this.doVariantBam(combineVCFsByType,BAMType.normal,"/datastore/bam/normal/"+this.normalBamGnosID+"/"+this.normalBAMFileName,this.normalBamGnosID, this.normalMinibamPath,this.normalBamGnosID);
			List<Job> parentJobsToAnnotationJobs = new ArrayList<Job>(this.tumours.size());

			//create a list of tumour variant-bam jobs.
			List<Job> variantBamJobs = new ArrayList<Job>(this.tumours.size()+1);
			for (int i = 0; i < this.tumours.size() ; i ++)
			{
				TumourInfo tInfo = this.tumours.get(i);
				Job tumourVariantBam = this.doVariantBam(combineVCFsByType,BAMType.tumour,"/datastore/bam/tumour/"+tInfo.getTumourBamGnosID()+"/"+tInfo.getTumourBAMFileName(), tInfo.getTumourBamGnosID(), tInfo.getTumourMinibamPath(),tInfo.getTumourBamGnosID());
				variantBamJobs.add(tumourVariantBam);
			}
			variantBamJobs.add(normalVariantBam);

			//Now that we've built our list of variantbam and oxog jobs, we can set up the proper parent-child relationships between them.
			//The idea is to run 1 OxoG at the same time as 2 variantbam jobs.
			for (int i=2; i < Math.max(variantBamJobs.size(), variantBamJobs.size()); i+=2)
			{
				variantBamJobs.get(i).addParent(variantBamJobs.get(i-2));
				if (i+1 < variantBamJobs.size())
				{
					variantBamJobs.get(i+1).addParent(variantBamJobs.get(i-2));
				}
			}
//			for (int i=1; i < Math.max(oxogJobs.size(), oxogJobs.size()); i+=2)
//			{
//				oxogJobs.get(i).addParent(oxogJobs.get(i-1));
//			}

			//set up parent jobs to annotation jobs
			for (Job j : oxogJobs)
			{
				parentJobsToAnnotationJobs.add(j);
			}
			for (Job j : variantBamJobs)
			{
				parentJobsToAnnotationJobs.add(j);
			}
			List<Job> annotationJobs = this.doAnnotations( parentJobsToAnnotationJobs.toArray(new Job[parentJobsToAnnotationJobs.size()]));
			
			//Now do the Upload
			if (!skipUpload)
			{
				// indicate job is in uploading stage.
				Job move2uploading = this.gitMove( "running-jobs", "uploading-jobs", annotationJobs.toArray(new Job[annotationJobs.size()]));
				Job uploadResults = doUpload(move2uploading);
				// indicate job is complete.
				this.gitMove( "uploading-jobs", "completed-jobs", uploadResults);
			}
			else
			{
				this.gitMove( "running-jobs", "completed-jobs",annotationJobs.toArray(new Job[annotationJobs.size()]));
			}
			//System.out.println(this.filesForUpload);
		}
		catch (Exception e)
		{
			throw new RuntimeException ("Exception caught: "+e.getMessage(), e);
		}
	}
}
