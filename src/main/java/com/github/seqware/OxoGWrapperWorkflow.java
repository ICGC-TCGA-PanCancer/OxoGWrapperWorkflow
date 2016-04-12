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
import java.util.stream.Stream;

import com.github.seqware.downloaders.DownloaderBuilder;
import com.github.seqware.downloaders.GNOSDownloader;
import com.github.seqware.downloaders.ICGCStorageDownloader;
import com.github.seqware.downloaders.S3Downloader;

import net.sourceforge.seqware.pipeline.workflowV2.model.Job;

public class OxoGWrapperWorkflow extends BaseOxoGWrapperWorkflow {

	private Collector<String[], ?, Map<String, Object>> collectToMap = Collectors.toMap(kv -> kv[0], kv -> kv[1]);;

	private Predicate<VcfInfo> isSanger = p -> p.getOriginatingPipeline() == Pipeline.sanger;
	private Predicate<VcfInfo> isBroad = p -> p.getOriginatingPipeline() == Pipeline.broad;
	private Predicate<VcfInfo> isDkfzEmbl = p -> p.getOriginatingPipeline() == Pipeline.dkfz_embl;
	private Predicate<VcfInfo> isMuse = p -> p.getOriginatingPipeline() == Pipeline.muse;
	
	private Predicate<VcfInfo> isIndel = p -> p.getVcfType() == VCFType.indel; 
	private Predicate<VcfInfo> isSnv = p -> p.getVcfType() == VCFType.snv;
	private Predicate<VcfInfo> isSv = p -> p.getVcfType() == VCFType.sv;
	
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
	 * Defines what BAM types there are:
	 * <ul><li>normal</li><li>tumour</li></ul>
	 * @author sshorser
	 *
	 */
	enum BAMType{
		normal,tumour
	}
	
	public enum DownloadMethod
	{
		gtdownload, icgcStorageClient, s3
	}
	
	private List<VcfInfo> extractedSnvsFromIndels = new ArrayList<VcfInfo>();
	private List<VcfInfo> mergedVcfs = new ArrayList<VcfInfo>();
	private List<VcfInfo> normalizedIndels = new ArrayList<VcfInfo>();
	
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
		// ...but really, the workflow should not be modifying the collab.token file. Whoever's running the workflow should set what they want.
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
//		getBamCommandString = DownloadUtils.getFileCommandString(downloadMethod, outDir, bamType.toString(), this.storageSource, this.gtDownloadVcfKey, objectIDs);
		String moveToFailed = GitUtils.gitMoveCommand("downloading-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, this.getWorkflowBaseDir() + "/scripts/");
		getBamFileJob.setCommand("( "+getBamCommandString+" ) || "+moveToFailed);

		return getBamFileJob;
	}

	private Job getBAM(Job parentJob, DownloadMethod downloadMethod, BAMType bamType, List<String> objectIDs) {
		return getBAM(parentJob, downloadMethod, bamType, objectIDs.toArray(new String[objectIDs.size()]));
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
		System.out.printf("DEBUG: getVCF: "+downloadMethod + " ; "+ workflowName + " ; %s\n", objectIDs);
		if (objectIDs == null || objectIDs.length == 0)
		{
			throw new RuntimeException("Cannot have empty objectIDs!");
		}
		
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
//		getVCFCommand = DownloadUtils.getFileCommandString(downloadMethod, outDir, workflowName.toString(), this.storageSource, this.gtDownloadVcfKey, objectIDs);
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
	private Job preProcessIndelVCF(Job parent, Pipeline workflowName, String vcfName, String tumourAliquotID )
	{
		System.out.println("DEBUG: preProcessIndelVCF: "+workflowName+" ; "+vcfName + " ; "+tumourAliquotID);
		String outDir = "/datastore/vcf/"+workflowName;
		String normalizedINDELName = tumourAliquotID+ "_"+ workflowName+"_somatic.indel.pass-filtered.bcftools-norm.vcf.gz";
		String extractedSNVVCFName = tumourAliquotID+ "_"+ workflowName+"_somatic.indel.pass-filtered.bcftools-norm.extracted-snvs.vcf";
		String fixedIndel = vcfName.replace("indel.", "indel.fixed.").replace(".gz", ""); //...because the fixed indel will not be a gz file - at least not immediately.
		Job bcfToolsNormJob = this.getWorkflow().createBashJob("normalize "+workflowName+" Indels");
		String sedTab = "\\\"$(echo -e '\\t')\\\"";
		String runBCFToolsNormCommand = TemplateUtils.getRenderedTemplate(Arrays.stream(new String[][]{
			{ "sedTab", sedTab }, { "outDir", outDir }, { "vcfName", vcfName }, { "workflowName", workflowName.toString() } ,
			{ "refFile", this.refFile }, { "fixedIndel", fixedIndel }, { "normalizedINDELName", normalizedINDELName },
			{ "tumourAliquotID", tumourAliquotID }
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
		
		VcfInfo extractedSnv = new VcfInfo();
		extractedSnv.setFileName(outDir + "/"+extractedSNVVCFName+".gz");
		extractedSnv.setOriginatingPipeline(workflowName);
		extractedSnv.setVcfType(VCFType.snv);
		extractedSnv.setOriginatingTumourAliquotID(tumourAliquotID);
		this.extractedSnvsFromIndels.add(extractedSnv);
		
		VcfInfo normalizedIndel = new VcfInfo();
		normalizedIndel.setFileName(outDir + "/"+normalizedINDELName);
		normalizedIndel.setOriginatingPipeline(workflowName);
		normalizedIndel.setVcfType(VCFType.indel);
		normalizedIndel.setOriginatingTumourAliquotID(tumourAliquotID);
		this.normalizedIndels.add(normalizedIndel);

		return extractSNVFromIndel;
	}
	
	/**
	 * This will combine VCFs from different workflows by the same type. All INDELs will be combined into a new output file,
	 * all SVs will be combined into a new file, all SNVs will be combined into a new file. 
	 * @param parents
	 * @return
	 */
	private Job combineVCFsByType(String tumourAliquotID, Job ... parents)
	{
		Predicate<? super VcfInfo> matchesTumourAliquotID = p -> p.getOriginatingTumourAliquotID().equals(tumourAliquotID);
		
		Predicate<? super VcfInfo> isSangerSNV = isSanger.and(isSnv.and(matchesTumourAliquotID));
		Predicate<? super VcfInfo> isBroadSNV = isBroad.and(isSnv.and(matchesTumourAliquotID));
		Predicate<? super VcfInfo> isDkfzEmblSNV = isDkfzEmbl.and(isSnv.and(matchesTumourAliquotID));
		
		Predicate<? super VcfInfo> isSangerSV = isSanger.and(isSv.and(matchesTumourAliquotID));
		Predicate<? super VcfInfo> isBroadSV = isBroad.and(isSv.and(matchesTumourAliquotID));
		Predicate<? super VcfInfo> isDkfzEmblSV = isDkfzEmbl.and(isSv.and(matchesTumourAliquotID));
		
		Predicate<? super VcfInfo> isMuseSNV = isMuse.and(isSnv.and(matchesTumourAliquotID));
				
		String sangerSNV = (this.vcfs.stream().filter(isSangerSNV).findFirst().get()).getFileName();
		String broadSNV = (this.vcfs.stream().filter(isBroadSNV).findFirst().get()).getFileName();
		String dkfzEmblSNV = (this.vcfs.stream().filter(isDkfzEmblSNV).findFirst().get()).getFileName();
		String museSNV = (this.vcfs.stream().filter(isMuseSNV).findFirst().get()).getFileName();
		
		String normalizedSangerIndel = (this.normalizedIndels.stream().filter(isSangerSNV).findFirst().get()).getFileName();
		String normalizedBroadIndel = (this.normalizedIndels.stream().filter(isBroadSNV).findFirst().get()).getFileName();
		String normalizedDkfzEmblIndel = (this.normalizedIndels.stream().filter(isDkfzEmblSNV).findFirst().get()).getFileName();

		String sangerSV = (this.vcfs.stream().filter(isSangerSV).findFirst().get()).getFileName();
		String broadSV = (this.vcfs.stream().filter(isBroadSV).findFirst().get()).getFileName();
		String dkfzEmblSV = (this.vcfs.stream().filter(isDkfzEmblSV).findFirst().get()).getFileName();

		
		//Create symlinks to the files in the proper directory.
		Job prepVCFs = this.getWorkflow().createBashJob("create links to VCFs");
		String prepCommand = "";
		prepCommand+="\n ( ( [ -d /datastore/merged_vcfs ] || sudo mkdir /datastore/merged_vcfs/ ) && sudo chmod a+rw /datastore/merged_vcfs && \\\n"
		+"\n ln -s /datastore/vcf/"+Pipeline.sanger+"/"+this.sangerGnosID+"/"+sangerSNV+" /datastore/vcf/"+Pipeline.sanger+"_snv.vcf && \\\n"
		+" ln -s /datastore/vcf/"+Pipeline.broad+"/"+this.broadGnosID+"/"+broadSNV+" /datastore/vcf/"+Pipeline.broad+"_snv.vcf && \\\n"
		+" ln -s /datastore/vcf/"+Pipeline.dkfz_embl+"/"+this.dkfzemblGnosID+"/"+dkfzEmblSNV+" /datastore/vcf/"+Pipeline.dkfz_embl+"_snv.vcf && \\\n"
		+" ln -s /datastore/vcf/"+Pipeline.muse+"/"+this.museGnosID+"/"+museSNV+" /datastore/vcf/"+Pipeline.muse+"_snv.vcf && \\\n"
		+" ln -s "+normalizedSangerIndel+" /datastore/vcf/"+Pipeline.sanger+"_indel.vcf && \\\n"
		+" ln -s "+normalizedBroadIndel+" /datastore/vcf/"+Pipeline.broad+"_indel.vcf && \\\n"
		+" ln -s "+normalizedDkfzEmblIndel+" /datastore/vcf/"+Pipeline.dkfz_embl+"_indel.vcf && \\\n"
		+" ln -s /datastore/vcf/"+Pipeline.sanger+"/"+this.sangerGnosID+"/"+sangerSV+" /datastore/vcf/"+Pipeline.sanger+"_sv.vcf && \\\n"
		+" ln -s /datastore/vcf/"+Pipeline.broad+"/"+this.broadGnosID+"/"+broadSV+" /datastore/vcf/"+Pipeline.broad+"_sv.vcf && \\\n"
		+" ln -s /datastore/vcf/"+Pipeline.dkfz_embl+"/"+this.dkfzemblGnosID+"/"+dkfzEmblSV+" /datastore/vcf/"+Pipeline.dkfz_embl+"_sv.vcf ) ";
		
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
				//rename the merged VCFs to ensure they contain the correct aliquot IDs.
				+ " && cd /datastore/merged_vcfs/ "
				+ " && mv snv.clean.sorted.vcf snv."+tumourAliquotID+".clean.sorted.vcf "
				+ " && mv sv.clean.sorted.vcf sv."+tumourAliquotID+".clean.sorted.vcf "
				+ " && mv indel.clean.sorted.vcf indel."+tumourAliquotID+".clean.sorted.vcf ) || "+moveToFailed;

		vcfCombineJob.setCommand(combineCommand);
		vcfCombineJob.addParent(prepVCFs);
		
		//these files names are hard-coded in runVariantbam.template. So, either get rid of them here (if they're not used anywhere else),
		//or add them as parameters to the template.
		//TODO: merged VCFs must now be done on SETS of VCFs from the same tumour.
		VcfInfo mergedSnvVcf = new VcfInfo();
		mergedSnvVcf.setFileName("snv."+tumourAliquotID+".clean.sorted.vcf");
		mergedSnvVcf.setVcfType(VCFType.snv);
		mergedSnvVcf.setOriginatingTumourAliquotID(tumourAliquotID);
		VcfInfo mergedSvVcf = new VcfInfo();
		mergedSvVcf.setFileName("sv."+tumourAliquotID+".clean.sorted.vcf");
		mergedSvVcf.setVcfType(VCFType.sv);
		mergedSvVcf.setOriginatingTumourAliquotID(tumourAliquotID);
		VcfInfo mergedIndelVcf = new VcfInfo();
		mergedIndelVcf.setFileName("indel."+tumourAliquotID+".clean.sorted.vcf");
		mergedIndelVcf.setVcfType(VCFType.indel);
		mergedIndelVcf.setOriginatingTumourAliquotID(tumourAliquotID);
		this.mergedVcfs.add(mergedSnvVcf);
		this.mergedVcfs.add(mergedSvVcf);
		this.mergedVcfs.add(mergedIndelVcf);

		return vcfCombineJob;
	}
	
	/**
	 * Runs the OxoG filtering program inside the Broad's OxoG docker container. Output file(s) will be in /datastore/oxog_results/ and the working files will 
	 * be in /datastore/oxog_workspace
	 * @param parent
	 * @return
	 */
	private Job doOxoG(String pathToTumour, String tumourAliquotID, Job ...parents) {
		
		String moveToFailed = GitUtils.gitMoveCommand("running-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, this.getWorkflowBaseDir() + "/scripts/");
		Job runOxoGWorkflow = this.getWorkflow().createBashJob("run OxoG Filter for tumour "+tumourAliquotID); 
		Function<String,String> getFileName = (s) -> {  return s.substring(s.lastIndexOf("/")); };
		String pathToResults = "/datastore/oxog_results/tumour_"+tumourAliquotID+"/cga/fh/pcawg_pipeline/jobResults_pipette/jobs/"+this.normalAliquotID+"/links_for_gnos/annotate_failed_sites_to_vcfs/";
		String pathToUploadDir = "/datastore/files_for_upload/";
		
		Predicate<? super VcfInfo> matchesTumourAliquotID = p -> p.getOriginatingTumourAliquotID().equals(tumourAliquotID);
		
		Predicate<? super VcfInfo> isSangerSNV = isSanger.and(isSnv.and(matchesTumourAliquotID));
		Predicate<? super VcfInfo> isBroadSNV = isBroad.and(isSnv.and(matchesTumourAliquotID));
		Predicate<? super VcfInfo> isDkfzEmblSNV = isDkfzEmbl.and(isSnv.and(matchesTumourAliquotID));
		Predicate<? super VcfInfo> isMuseSNV = isMuse.and(isSnv.and(matchesTumourAliquotID));
				
		String sangerSNV = (this.vcfs.stream().filter(isSangerSNV).findFirst().get()).getFileName();
		String broadSNV = (this.vcfs.stream().filter(isBroadSNV).findFirst().get()).getFileName();
		String dkfzEmblSNV = (this.vcfs.stream().filter(isDkfzEmblSNV).findFirst().get()).getFileName();
		String museSNV = (this.vcfs.stream().filter(isMuseSNV).findFirst().get()).getFileName();
		
		String extractedSangerSNV = (this.extractedSnvsFromIndels.stream().filter(isSangerSNV).findFirst().get()).getFileName();
		String extractedBroadSNV = (this.extractedSnvsFromIndels.stream().filter(isBroadSNV).findFirst().get()).getFileName();
		String extractedDkfzEmblSNV = (this.extractedSnvsFromIndels.stream().filter(isDkfzEmblSNV).findFirst().get()).getFileName();
		
		String normalizedSangerIndel = (this.normalizedIndels.stream().filter(isSangerSNV).findFirst().get()).getFileName();
		String normalizedBroadIndel = (this.normalizedIndels.stream().filter(isBroadSNV).findFirst().get()).getFileName();
		String normalizedDkfzEmblIndel = (this.normalizedIndels.stream().filter(isDkfzEmblSNV).findFirst().get()).getFileName();
		
		if (!this.skipOxoG)
		{
			String runOxoGCommand = TemplateUtils.getRenderedTemplate(Arrays.stream(new String[][] {
					{ "sangerExtractedSNVVCFPath", extractedSangerSNV }, { "sangerWorkflow", Pipeline.sanger.toString() }, { "sangerExtractedSNVVCF", getFileName.apply(extractedSangerSNV) },
					{ "broadExtractedSNVVCFPath", extractedBroadSNV }, { "broadWorkflow", Pipeline.broad.toString() }, { "broadExtractedSNVVCF", getFileName.apply(extractedBroadSNV) },
					{ "dkfzEmblExtractedSNVVCFPath", extractedDkfzEmblSNV }, { "dkfzEmblWorkflow", Pipeline.dkfz_embl.toString() }, { "dkfzEmblExtractedSNVVCF", getFileName.apply(extractedDkfzEmblSNV) },
					{ "tumourID", tumourAliquotID }, { "aliquotID", this.normalAliquotID }, { "oxoQScore", this.oxoQScore }, { "museWorkflow", Pipeline.muse.toString() },
					{ "pathToTumour", pathToTumour }, { "normalBamGnosID", this.normalBamGnosID }, { "normalBAMFileName", this.normalBAMFileName } ,
					{ "broadGnosID", this.broadGnosID }, { "sangerGnosID", this.sangerGnosID }, { "dkfzemblGnosID", this.dkfzemblGnosID }, { "museGnosID", this.museGnosID },
					{ "sangerSNVName", sangerSNV}, { "broadSNVName", broadSNV }, { "dkfzEmblSNVName", dkfzEmblSNV }, { "museSNVName", museSNV },
					{ "pathToResults", pathToResults}, { "pathToUploadDir", pathToUploadDir },
					{ "broadNormalizedIndelVCFName",normalizedBroadIndel }, { "sangerNormalizedIndelVCFName",normalizedSangerIndel },
					{ "dkfzEmblNormalizedIndelVCFName",normalizedDkfzEmblIndel }
				} ).collect(this.collectToMap), "runOxoGFilter.template");
			runOxoGWorkflow.setCommand("( "+runOxoGCommand+" ) || "+ moveToFailed);
		}
		for (Job parent : parents)
		{
			runOxoGWorkflow.addParent(parent);
		}

		Function<String,String> changeToOxoGSuffix = (s) -> {return pathToUploadDir + s.replace(".vcf.gz", ".oxoG.vcf.gz"); };
		Function<String,String> changeToOxoGTBISuffix = changeToOxoGSuffix.andThen((s) -> s+=".tbi"); 
		//regular VCFs
		this.filesForUpload.add(changeToOxoGSuffix.apply(broadSNV));
		this.filesForUpload.add(changeToOxoGSuffix.apply(dkfzEmblSNV));
		this.filesForUpload.add(changeToOxoGSuffix.apply(sangerSNV));
		this.filesForUpload.add(changeToOxoGSuffix.apply(museSNV));
		//index files
		this.filesForUpload.add(changeToOxoGTBISuffix.apply(broadSNV));
		this.filesForUpload.add(changeToOxoGTBISuffix.apply(dkfzEmblSNV));
		this.filesForUpload.add(changeToOxoGTBISuffix.apply(sangerSNV));
		this.filesForUpload.add(changeToOxoGTBISuffix.apply(museSNV));
		//Extracted SNVs
		this.filesForUpload.add(changeToOxoGSuffix.apply(getFileName.apply(extractedSangerSNV)));
		this.filesForUpload.add(changeToOxoGSuffix.apply(getFileName.apply(extractedBroadSNV)));
		this.filesForUpload.add(changeToOxoGSuffix.apply(getFileName.apply(extractedDkfzEmblSNV)));
		//index files
		this.filesForUpload.add(changeToOxoGTBISuffix.apply(getFileName.apply(extractedSangerSNV)));
		this.filesForUpload.add(changeToOxoGTBISuffix.apply(getFileName.apply(extractedBroadSNV)));
		this.filesForUpload.add(changeToOxoGTBISuffix.apply(getFileName.apply(extractedDkfzEmblSNV)));
		
		this.filesForUpload.add("/datastore/files_for_upload/" + this.normalAliquotID + ".gnos_files_tumour_" + tumourAliquotID + ".tar");
		this.filesForUpload.add("/datastore/files_for_upload/" + this.normalAliquotID + ".call_stats_tumour_" + tumourAliquotID + ".txt.gz.tar");
		
		return runOxoGWorkflow;
		
	}

	private Job doVariantBam(BAMType bamType, String bamPath,  Job ...parents) {
		return doVariantBam(bamType, bamPath,  null, null, parents);
	}
	
	/**
	 * Runs the variant program inside the Broad's OxoG container to produce a mini-BAM for a given BAM. 
	 * @param parent
	 * @param bamType - The type of BAM file to use. Determines the name of the output file.
	 * @param bamPath - The path to the input BAM file.
	 * @param tumourBAMFileName - Name of the BAM file. Only used if bamType == BAMType.tumour.
	 * @param tumourID - GNOS ID of the tumour. Only used if bamType == BAMType.tumour.
	 * @return
	 */
	private Job doVariantBam(BAMType bamType, String bamPath, String tumourBAMFileName, String tumourID, Job ...parents) {
		Job runVariantbam = this.getWorkflow().createBashJob("run "+bamType+(bamType==BAMType.tumour?"_"+tumourID+"_":"")+" variantbam");

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
			String tumourMinibamPath = "/datastore/variantbam_results/"+minibamName+".bam";
			
			for (int i = 0; i < this.tumours.size(); i++ )
			{
				if (this.tumours.get(i).getAliquotID().equals(tumourID))
				{
					this.tumours.get(i).setTumourMinibamPath(tumourMinibamPath);
				}
			}
			
			this.filesForUpload.add(tumourMinibamPath);
			this.filesForUpload.add(tumourMinibamPath+".bai");
		}
		
		if (!this.skipVariantBam)
		{
			String command = TemplateUtils.getRenderedTemplate(Arrays.stream( new String[][] {
				{ "containerNameSuffix", bamType + (bamType == BAMType.tumour ? "_with_tumour_"+tumourID:"") },
				{ "minibamName", minibamName+".bam"},  {"snvPadding", String.valueOf(this.snvPadding)}, {"svPadding", String.valueOf(this.svPadding)},
				{ "indelPadding", String.valueOf(this.indelPadding) }, { "pathToBam", bamPath }
			}).collect(this.collectToMap), "runVariantbam.template" );
			
			String moveToFailed = GitUtils.gitMoveCommand("running-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, this.getWorkflowBaseDir() + "/scripts/");
			command += (" || " + moveToFailed);
			runVariantbam.setCommand(command);
		}
		for (Job parent : parents)
		{
			runVariantbam.addParent(parent);
		}
		
		//Job getLogs = this.getOxoGLogs(runOxoGWorkflow);
		return runVariantbam;
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
				{ "normalMetadataURL", this.normalMetdataURL } , { "tumourMetadataURLs", String.join("," , this.tumours.stream().map(t -> t.getTumourMetdataURL()).collect(Collectors.toList()) ) },
				{ "bams", bams }, { "bamIndicies", bamIndicies}, { "bamMD5Sums", bamMD5Sums }, { "bamIndexMD5Sums", bamIndexMD5Sums}, 
				{ "studyRefNameOverride", this.studyRefNameOverride }, { "workflowVersion", this.getVersion() } 
			}).collect(this.collectToMap),"generateBAMAnalysisMetadata.template");

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
			}).collect(this.collectToMap), "analysisVCFDescription.template");

		
		generateAnalysisFilesVCFCommand += TemplateUtils.getRenderedTemplate(Arrays.stream(new String[][] {
				{ "gnosKey", this.gnosKey }, { "gnosMetadataUploadURL", this.gnosMetadataUploadURL }, { "vcfDescription", vcfDescription },
				{ "normalMetadataURL", this.normalMetdataURL } , { "tumourMetadataURLs", String.join("," , this.tumours.stream().map(t -> t.getTumourMetdataURL()).collect(Collectors.toList()) ) },
				{ "vcfs", vcfs }, { "tars", tars }, { "tarMD5Sums", tarMD5Sums }, { "vcfIndicies", vcfIndicies}, { "vcfMD5Sums", vcfMD5Sums },
				{ "vcfIndexMD5Sums", vcfIndexMD5Sums}, { "studyRefNameOverride", this.studyRefNameOverride }, { "workflowVersion", this.getVersion() } 
			}).collect(this.collectToMap),"generateVCFAnalysisMetadata.template");
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
	private Job runAnnotator(String inputType, Pipeline workflowName, String vcfPath, String tumourBamPath, String normalBamPath, String tumourAliquotID, Job ...parents)
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
		final String passFilteredOxoGSuffix = ".pass-filtered.oxoG.vcf.gz";
		//list filtering should only ever produce one result.
		
		for (int i = 0; i < this.tumours.size(); i++)
		{
			TumourInfo tInf = this.tumours.get(i);
			String tumourAliquotID = tInf.getAliquotID();
			String broadOxogSNVFileName = this.filesForUpload.stream().filter(p -> ((p.contains(tumourAliquotID) && p.contains("broad-mutect") && p.endsWith(passFilteredOxoGSuffix)))).collect(Collectors.toList()).get(0);
			String broadOxoGSNVFromIndelFileName = this.filesForUpload.stream().filter(p -> (p.contains(Pipeline.broad.toString()) && isExtractedSNV.test(p) )).collect(Collectors.toList()).get(0);
			
			String sangerOxogSNVFileName = this.filesForUpload.stream().filter(p -> ((p.contains(tumourAliquotID) && p.contains("svcp_") && p.endsWith(passFilteredOxoGSuffix)))).collect(Collectors.toList()).get(0);
			String sangerOxoGSNVFromIndelFileName = this.filesForUpload.stream().filter(p -> (p.contains(Pipeline.sanger.toString()) && isExtractedSNV.test(p) )).collect(Collectors.toList()).get(0);
			
			String dkfzEmbleOxogSNVFileName = this.filesForUpload.stream().filter(p -> ((p.contains(tumourAliquotID) && p.contains("dkfz-snvCalling") && p.endsWith(passFilteredOxoGSuffix)))).collect(Collectors.toList()).get(0);
			String dkfzEmblOxoGSNVFromIndelFileName = this.filesForUpload.stream().filter(p -> (p.contains(Pipeline.dkfz_embl.toString()) && isExtractedSNV.test(p) )).collect(Collectors.toList()).get(0);

			//Remember: MUSE files do not get PASS-filtered. Also, there is no INDEL so there cannot be any SNVs extracted from INDELs.
			String museOxogSNVFileName = this.filesForUpload.stream().filter(p -> p.contains("MUSE") && p.endsWith("somatic.snv_mnv.oxoG.vcf.gz")).collect(Collectors.toList()).get(0);
			
			String normalizedBroadIndel = this.normalizedIndels.stream().filter(isBroad.and(p -> p.getOriginatingTumourAliquotID().equals(tumourAliquotID))).findFirst().get().getFileName();
			String normalizedSangerIndel = this.normalizedIndels.stream().filter(isSanger.and(p -> p.getOriginatingTumourAliquotID().equals(tumourAliquotID))).findFirst().get().getFileName();
			String normalizedDkfzEmblIndel = this.normalizedIndels.stream().filter(isDkfzEmbl.and(p -> p.getOriginatingTumourAliquotID().equals(tumourAliquotID))).findFirst().get().getFileName();
			
			Job broadIndelAnnotatorJob = this.runAnnotator("indel", Pipeline.broad, normalizedBroadIndel, tInf.getTumourMinibamPath(),this.normalMinibamPath, tInf.getAliquotID(), parents);
			Job dfkzEmblIndelAnnotatorJob = this.runAnnotator("indel", Pipeline.dkfz_embl, normalizedDkfzEmblIndel, tInf.getTumourMinibamPath(), this.normalMinibamPath, tInf.getAliquotID(), broadIndelAnnotatorJob);
			Job sangerIndelAnnotatorJob = this.runAnnotator("indel", Pipeline.sanger, normalizedSangerIndel, tInf.getTumourMinibamPath(), this.normalMinibamPath, tInf.getAliquotID(), dfkzEmblIndelAnnotatorJob);
	
			Job broadSNVAnnotatorJob = this.runAnnotator("SNV", Pipeline.broad,broadOxogSNVFileName, tInf.getTumourMinibamPath(), this.normalMinibamPath, tInf.getAliquotID(), parents);
			Job dfkzEmblSNVAnnotatorJob = this.runAnnotator("SNV", Pipeline.dkfz_embl,dkfzEmbleOxogSNVFileName, tInf.getTumourMinibamPath(), this.normalMinibamPath, tInf.getAliquotID(), broadSNVAnnotatorJob);
			Job sangerSNVAnnotatorJob = this.runAnnotator("SNV",Pipeline.sanger,sangerOxogSNVFileName, tInf.getTumourMinibamPath(), this.normalMinibamPath, tInf.getAliquotID(), dfkzEmblSNVAnnotatorJob);
			Job museSNVAnnotatorJob = this.runAnnotator("SNV",Pipeline.muse,museOxogSNVFileName, tInf.getTumourMinibamPath(), this.normalMinibamPath, tInf.getAliquotID(), dfkzEmblSNVAnnotatorJob);
	
			Job broadSNVFromIndelAnnotatorJob = this.runAnnotator("SNV",Pipeline.broad, broadOxoGSNVFromIndelFileName, tInf.getTumourMinibamPath(), this.normalMinibamPath, tInf.getAliquotID(), parents);
			Job dfkzEmblSNVFromIndelAnnotatorJob = this.runAnnotator("SNV",Pipeline.dkfz_embl, dkfzEmblOxoGSNVFromIndelFileName, tInf.getTumourMinibamPath(), this.normalMinibamPath, tInf.getAliquotID(), broadSNVFromIndelAnnotatorJob);
			Job sangerSNVFromIndelAnnotatorJob = this.runAnnotator("SNV",Pipeline.sanger, sangerOxoGSNVFromIndelFileName, tInf.getTumourMinibamPath(), this.normalMinibamPath, tInf.getAliquotID(), dfkzEmblSNVFromIndelAnnotatorJob);

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

		for (VcfInfo vcfInfo : this.vcfs)
		{
			statFilesCMD+="stat /datastore/vcf/"+vcfInfo.getOriginatingPipeline().toString()+"/"+vcfInfo.getPipelineGnosID()+"/"+vcfInfo.getFileName()+ " && \\\n";
			statFilesCMD+="stat /datastore/vcf/"+vcfInfo.getOriginatingPipeline().toString()+"/"+vcfInfo.getPipelineGnosID()+"/"+vcfInfo.getIndexFileName()+ " && \\\n";
		}
		
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
			
			Job installTabix = this.installTools(copy);
			
			// indicate job is in downloading stage.
			String pathToScripts = this.getWorkflowBaseDir() + "/scripts";
			Job move2download = GitUtils.gitMove("queued-jobs", "downloading-jobs", this.getWorkflow(), this.JSONlocation, this.JSONrepoName, this.JSONfolderName, this.GITname, this.GITemail, this.gitMoveTestMode, this.JSONfileName, pathToScripts ,installTabix, pullRepo);
			Job move2running;
			if (!skipDownload) {
				//Download jobs. VCFs downloading serial. Trying to download all in parallel seems to put too great a strain on the system 
				//since the icgc-storage-client can make full use of all cores on a multi-core system. 
				DownloadMethod downloadMethod = DownloadMethod.valueOf(this.downloadMethod);
				
				List<String> sangerList =  Stream.concat(
						this.vcfs.stream().filter(isSanger).map(m -> m.getObjectID()), 
						this.vcfs.stream().filter(isSanger).map(m -> m.getIndexObjectID())
					).collect(Collectors.toList()) ;
				List<String> broadList = Stream.concat(
						this.vcfs.stream().filter(isBroad).map(m -> m.getObjectID()), 
						this.vcfs.stream().filter(isBroad).map(m -> m.getIndexObjectID())
					).collect(Collectors.toList()) ;
				List<String> dkfzEmblList = Stream.concat(
						this.vcfs.stream().filter(isDkfzEmbl).map(m -> m.getObjectID()), 
						this.vcfs.stream().filter(isDkfzEmbl).map(m -> m.getIndexObjectID())
					).collect(Collectors.toList()) ;
				List<String> museList = Stream.concat(
						this.vcfs.stream().filter(isMuse).map(m -> m.getObjectID()), 
						this.vcfs.stream().filter(isMuse).map(m -> m.getIndexObjectID())
					).collect(Collectors.toList()) ;
				List<String> normalList = Arrays.asList( this.bamNormalIndexObjectID,this.bamNormalObjectID);

				//System.out.println("DEBUG: sangerList: "+sangerList.toString());
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
					workflowObjectIDs.put(BAMType.tumour+"_"+tInfo.getAliquotID(), tumourIDs);
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
				System.out.println("DEBUG: "+workflowObjectIDs);
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
						downloadTumourBam = this.getBAM(downloadNormalBam, downloadMethod, BAMType.tumour,chooseObjects.apply( BAMType.tumour.toString()+"_"+this.tumours.get(i).getAliquotID() ) );
					}
					else
					{
						downloadTumourBam = this.getBAM(getTumourJobs.get(i-1), downloadMethod, BAMType.tumour,chooseObjects.apply( BAMType.tumour.toString()+"_"+this.tumours.get(i).getAliquotID() ) );
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
			
			
			for (VcfInfo vInfo : this.vcfs)
			{
				vInfo.setFileName(addPassFilteredSuffix.apply( vInfo.getFileName() ) );
			}
			
			// OxoG will run after move2running. Move2running will run after all the jobs that perform input file downloads and file preprocessing have finished.
			
			List<Job> preprocessJobs = new ArrayList<Job>(this.tumours.size() * 3);
			for (int i =0; i < this.tumours.size(); i++)
			{
			
				Job sangerPreprocessVCF = this.preProcessIndelVCF(sangerPassFilter, Pipeline.sanger,"/"+ this.sangerGnosID +"/"+ this.vcfs.stream().filter(isSanger.and(isIndel))
																																					.map(m -> m.getFileName()),
																																this.tumours.get(i).getAliquotID());
				Job dkfzEmblPreprocessVCF = this.preProcessIndelVCF(dkfzemblPassFilter, Pipeline.dkfz_embl, "/"+ this.dkfzemblGnosID +"/"+ this.vcfs.stream().filter(isDkfzEmbl.and(isIndel))
																																							.map(m -> m.getFileName()),
																																this.tumours.get(i).getAliquotID());
				Job broadPreprocessVCF = this.preProcessIndelVCF(broadPassFilter, Pipeline.broad, "/"+ this.broadGnosID +"/"+ this.vcfs.stream().filter(isBroad.and(isIndel))
																																				.map(m -> m.getFileName()),
																																this.tumours.get(i).getAliquotID());
				preprocessJobs.add(broadPreprocessVCF);
				preprocessJobs.add(sangerPreprocessVCF);
				preprocessJobs.add(dkfzEmblPreprocessVCF);
			}
			List<Job> combineVCFJobs = new ArrayList<Job>(this.tumours.size());
			for (int i =0; i< this.tumours.size(); i++)
			{		
				Job combineVCFsByType = this.combineVCFsByType(this.tumours.get(i).getAliquotID(), preprocessJobs.toArray(new Job[preprocessJobs.size()]) );
				combineVCFJobs.add(combineVCFsByType);
			}
			
			List<Job> oxogJobs = new ArrayList<Job>(this.tumours.size());
			for (int i = 0 ; i < this.tumours.size(); i++)
			{
				TumourInfo tInf = this.tumours.get(i);
				Job oxoG; 
				if (i>0)
				{
					//OxoG jobs can put a heavy CPU load on the system (in bursts) so probably better to run them in sequence.
					//If there is > 1 OxoG job (i.e. a multi-tumour donor), each OxoG job should have the prior OxoG job as its parent.
					oxoG = this.doOxoG(tInf.getTumourBamGnosID()+"/"+tInf.getTumourBAMFileName(),tInf.getAliquotID(), oxogJobs.get(i-1));
					for (int j =0 ; j<combineVCFJobs.size(); j++)
					{
						oxoG.addParent(combineVCFJobs.get(j));
					}
				}
				else
				{
					oxoG = this.doOxoG(tInf.getTumourBamGnosID()+"/"+tInf.getTumourBAMFileName(),tInf.getAliquotID());
					for (int j =0 ; j<combineVCFJobs.size(); j++)
					{
						oxoG.addParent(combineVCFJobs.get(j));
					}
				}
				oxogJobs.add(oxoG);
			}
			
			Job normalVariantBam = this.doVariantBam(BAMType.normal,"/datastore/bam/normal/"+this.normalBamGnosID+"/"+this.normalBAMFileName,combineVCFJobs.toArray(new Job[combineVCFJobs.size()]));
			List<Job> parentJobsToAnnotationJobs = new ArrayList<Job>(this.tumours.size());

			//create a list of tumour variant-bam jobs.
			List<Job> variantBamJobs = new ArrayList<Job>(this.tumours.size()+1);
			for (int i = 0; i < this.tumours.size() ; i ++)
			{
				TumourInfo tInfo = this.tumours.get(i);
				Job tumourVariantBam = this.doVariantBam(BAMType.tumour,"/datastore/bam/tumour/"+tInfo.getTumourBamGnosID()+"/"+tInfo.getTumourBAMFileName(), tInfo.getTumourBAMFileName(), tInfo.getAliquotID(),combineVCFJobs.toArray(new Job[combineVCFJobs.size()]));
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
