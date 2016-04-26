package com.github.seqware;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
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
	
	Supplier<? extends String> emptyStringWhenMissingFilesAllowed = () -> this.allowMissingFiles ? "" : null;
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
	
	@FunctionalInterface
	interface QuadConsumer<A,B,C,D>
	{
		void accept(A a, B b, C c, D d);
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
	
	private String getFileCommandString(DownloadMethod downloadMethod, String outDir, String downloadType, String storageSource, String downloadKey, String ... objectIDs  )
	{
		switch (downloadMethod)
		{
			case icgcStorageClient:
				//System.out.println("DEBUG: storageSource: "+this.storageSource);
				return ( DownloaderBuilder.of(ICGCStorageDownloader::new).with(ICGCStorageDownloader::setStorageSource, storageSource).build() ).getDownloadCommandString(outDir, downloadType, objectIDs);
			case gtdownload:
				//System.out.println("DEBUG: gtDownloadKey: "+this.gtDownloadVcfKey);
				return ( DownloaderBuilder.of(GNOSDownloader::new).with(GNOSDownloader::setDownloadKey, downloadKey).build() ).getDownloadCommandString(outDir, downloadType, objectIDs);
			case s3:
				return ( DownloaderBuilder.of(S3Downloader::new).build() ).getDownloadCommandString(outDir, downloadType, objectIDs);
			default:
				throw new RuntimeException("Unknown downloadMethod: "+downloadMethod.toString());
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
		getBamCommandString = getFileCommandString(downloadMethod, outDir, bamType.toString(), this.storageSource, this.gtDownloadVcfKey, objectIDs);
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
		//System.out.printf("DEBUG: getVCF: "+downloadMethod + " ; "+ workflowName + " ; %s\n", objectIDs);
		if (objectIDs == null || objectIDs.length == 0)
		{
			throw new RuntimeException("Cannot have empty objectIDs!");
		}
		
		Job getVCFJob = this.getWorkflow().createBashJob("get VCF for workflow " + workflowName);
		String outDir = "/datastore/vcf/"+workflowName;
		String moveToFailed = GitUtils.gitMoveCommand("downloading-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, this.getWorkflowBaseDir() + "/scripts/");
		String getVCFCommand ;
		getVCFCommand = getFileCommandString(downloadMethod, outDir, workflowName.toString(), this.storageSource, this.gtDownloadVcfKey, objectIDs);
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
		Job installTabixJob = this.getWorkflow().createBashJob("install tools");
		
		installTabixJob.setCommand("sudo apt-get install tabix libstring-random-perl -y ");
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
		
		//Create symlinks to the files in the proper directory.
		Job prepVCFs = this.getWorkflow().createBashJob("create links to VCFs");
		String prepCommand = "";
		prepCommand+="\n ( ( [ -d /datastore/merged_vcfs ] || sudo mkdir -p /datastore/merged_vcfs/ ) && sudo chmod a+rw /datastore/merged_vcfs && \\\n";
		
		String combineVcfArgs = "";
		
		for (VcfInfo vcfInfo : this.vcfs.stream().filter(p -> p.getOriginatingTumourAliquotID().equals(tumourAliquotID) && isIndel.negate().test(p)).collect(Collectors.toList()))
		{
			prepCommand += " ln -s /datastore/vcf/"+vcfInfo.getOriginatingPipeline().toString()+"/"+vcfInfo.getPipelineGnosID()+"/"+vcfInfo.getFileName()+" "
								+ " /datastore/vcf/"+vcfInfo.getOriginatingTumourAliquotID()+"_"+vcfInfo.getOriginatingPipeline().toString()+"_"+vcfInfo.getVcfType().toString()+".vcf && \\\n";
			combineVcfArgs += " --" + vcfInfo.getOriginatingPipeline().toString() + "_" + vcfInfo.getVcfType().toString()+
								" "+vcfInfo.getOriginatingTumourAliquotID() + "_" + vcfInfo.getOriginatingPipeline().toString() + "_"+vcfInfo.getVcfType().toString()+".vcf \\\n";
		}

		for (VcfInfo vcfInfo : this.normalizedIndels.stream().filter(p -> p.getOriginatingTumourAliquotID().equals(tumourAliquotID)).collect(Collectors.toList()))
		{
			prepCommand += " ln -s "+vcfInfo.getFileName()+" /datastore/vcf/"+vcfInfo.getOriginatingTumourAliquotID()+"_"+vcfInfo.getOriginatingPipeline().toString()+"_"+vcfInfo.getVcfType().toString()+".vcf && \\\n";
			combineVcfArgs += " --" + vcfInfo.getOriginatingPipeline().toString() + "_" + vcfInfo.getVcfType().toString()+
								" "+vcfInfo.getOriginatingTumourAliquotID() + "_" + vcfInfo.getOriginatingPipeline().toString() + "_"+vcfInfo.getVcfType().toString()+".vcf \\\n";
		}
		prepCommand = prepCommand.substring(0,prepCommand.lastIndexOf("&&"));
		String moveToFailed = GitUtils.gitMoveCommand("running-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, this.getWorkflowBaseDir() + "/scripts/");
		prepCommand += (") || " + moveToFailed);
		
		prepVCFs.setCommand(prepCommand);
		
		for (Job parent : parents)
		{
			prepVCFs.addParent(parent);
		}
		
		Job vcfCombineJob = this.getWorkflow().createBashJob("combining VCFs by type for tumour "+tumourAliquotID);
		
		//run the merge script, then bgzip and index them all.
		String combineCommand = "(sudo mkdir -p /datastore/merged_vcfs/"+tumourAliquotID+"/ "
								+ " && sudo chmod a+rw /datastore/merged_vcfs/"+tumourAliquotID+"/ "
				+ " && perl "+this.getWorkflowBaseDir()+"/scripts/vcf_merge_by_type.pl "
				+ combineVcfArgs
				+ " --indir /datastore/vcf/ --outdir /datastore/merged_vcfs/"+tumourAliquotID+"/ \\\n"
				//rename the merged VCFs to ensure they contain the correct aliquot IDs.
				+ " && cd /datastore/merged_vcfs/"+tumourAliquotID+"/ \\\n"
				+ " && cp snv.clean.sorted.vcf ../snv."+tumourAliquotID+".clean.sorted.vcf \\\n"
				+ " && cp sv.clean.sorted.vcf ../sv."+tumourAliquotID+".clean.sorted.vcf \\\n"
				+ " && cp indel.clean.sorted.vcf ../indel."+tumourAliquotID+".clean.sorted.vcf \\\n) || "+moveToFailed;

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

	private String getVcfName(Predicate<? super VcfInfo> vcfPredicate, List<VcfInfo> vcfList) {
		if (this.allowMissingFiles)
		{
			VcfInfo dummy = new VcfInfo();
			dummy.setFileName("");
			dummy.setIndexFileName("");
			dummy.setObjectID("");
			dummy.setIndexObjectID("");
			VcfInfo v = vcfList.stream().filter(vcfPredicate).findFirst().orElse( dummy );
			return v.getFileName();
		}
		else
		{
			return vcfList.stream().filter(vcfPredicate).findFirst().get().getFileName();
		}
	}
	
	private Predicate<? super VcfInfo> vcfMatchesTypePipelineTumour(Predicate<VcfInfo> vcfPredicate, Predicate<VcfInfo> pipelinePredicate, String tumourAliquotID)
	{
		return pipelinePredicate.and(vcfPredicate).and(this.matchesTumour(tumourAliquotID));
	}
	
	private Predicate<? super VcfInfo> matchesTumour(String tumourAliquotID) {
		return p -> p.getOriginatingTumourAliquotID().equals(tumourAliquotID);
	}
	
	/**
	 * Runs the OxoG filtering program inside the Broad's OxoG docker container. Output file(s) will be in /datastore/oxog_results/tumour_${tumourAliquotID} and the working files will 
	 * be in /datastore/oxog_workspace/tumour_${tumourAliquotID}
	 * @param parent
	 * @return
	 */
	private Job doOxoG(String pathToTumour, String tumourAliquotID, Job ...parents) {
		
		String moveToFailed = GitUtils.gitMoveCommand("running-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, this.getWorkflowBaseDir() + "/scripts/");
		Job runOxoGWorkflow = this.getWorkflow().createBashJob("run OxoG Filter for tumour "+tumourAliquotID);
		Function<String,String> getFileName = s -> s.substring(s.lastIndexOf("/")); 
		
		BiFunction<String,Pipeline,String> generatePathForOxoG = (vcfName,pipeline) -> {
			if (this.allowMissingFiles)
			{
				if (vcfName!=null && !vcfName.trim().equals(""))
				{
					return "/datafiles/VCF/"+pipeline+"/"+vcfName.substring(vcfName.lastIndexOf("/"));
				}
				return "";
			}
			return "/datafiles/VCF/"+pipeline+"/"+vcfName.substring(vcfName.lastIndexOf("/"));
		};
		
		String pathToResults = "/datastore/oxog_results/tumour_"+tumourAliquotID+"/cga/fh/pcawg_pipeline/jobResults_pipette/jobs/"+this.normalAliquotID+"/links_for_gnos/annotate_failed_sites_to_vcfs/";
		String pathToUploadDir = "/datastore/files_for_upload/";
		
		Predicate<? super VcfInfo> isSangerSNV = this.vcfMatchesTypePipelineTumour(isSnv, isSanger, tumourAliquotID);
		Predicate<? super VcfInfo> isBroadSNV = this.vcfMatchesTypePipelineTumour(isSnv, isBroad, tumourAliquotID);
		Predicate<? super VcfInfo> isDkfzEmblSNV = this.vcfMatchesTypePipelineTumour(isSnv, isDkfzEmbl, tumourAliquotID);
		Predicate<? super VcfInfo> isMuseSNV = this.vcfMatchesTypePipelineTumour(isSnv, isMuse, tumourAliquotID);
		
		BiFunction<String,String,String> applyPrefixForOxoG = (vcfName,pipeline) -> {
			if (vcfName == null || vcfName.trim().length()==0)
			{
				//if missing files are allowed, just return an empty string.
				if (this.allowMissingFiles)
				{
					return "";
				}
				else
				{
					throw new RuntimeException("A VCF name was empty/missing, but allowMissingFiles is "+this.allowMissingFiles);
				}
			}
			else
			{
				// This is the path inside the OxoG container that a VCF must be on.
				return "/datafiles/VCF/"+pipeline+"/"+vcfName;
			}
		};
		
		String sangerSNV = applyPrefixForOxoG.apply(this.getVcfName(isSangerSNV,this.vcfs),Pipeline.sanger.toString());
		String broadSNV = applyPrefixForOxoG.apply(this.getVcfName(isBroadSNV,this.vcfs),Pipeline.broad.toString());
		String dkfzEmblSNV = applyPrefixForOxoG.apply(this.getVcfName(isDkfzEmblSNV,this.vcfs),Pipeline.dkfz_embl.toString());
		String museSNV = applyPrefixForOxoG.apply(this.getVcfName(isMuseSNV,this.vcfs),Pipeline.muse.toString());
		
		String extractedSangerSNV = this.getVcfName(isSangerSNV,this.extractedSnvsFromIndels);
		String extractedBroadSNV = this.getVcfName(isBroadSNV,this.extractedSnvsFromIndels);
		String extractedDkfzEmblSNV = this.getVcfName(isDkfzEmblSNV,this.extractedSnvsFromIndels);
		
		if (!this.skipOxoG)
		{
			String checkSangerExtractedSNV;
			String checkBroadExtractedSNV;
			String checkDkfzEmblExtractedSNV;
			BiFunction<String,Pipeline,String> getCheckExtractedSNVString = (vcf,pipeline) -> {
				if (!this.allowMissingFiles || (this.allowMissingFiles && vcf!=null && vcf.trim().length()>0))
				{
						return TemplateUtils.getRenderedTemplate(Arrays.stream(new String[][]{
							{ "extractedSNVVCF", vcf}, { "worfklow", "sanger" }, {"extractedSNVVCFPath",generatePathForOxoG.apply(vcf,pipeline) }
						}).collect(this.collectToMap),"checkForExtractedSNVs.template");
				}
				else
				{
					return "";
				}
			};

			checkSangerExtractedSNV = getCheckExtractedSNVString.apply(extractedSangerSNV, Pipeline.sanger);
			checkBroadExtractedSNV = getCheckExtractedSNVString.apply(extractedBroadSNV, Pipeline.broad);
			checkDkfzEmblExtractedSNV = getCheckExtractedSNVString.apply(extractedDkfzEmblSNV, Pipeline.dkfz_embl);
			
			String runOxoGCommand = TemplateUtils.getRenderedTemplate(Arrays.stream(new String[][] {
					{ "checkSangerExtractedSNV" , checkSangerExtractedSNV },{ "checkBroadExtractedSNV" , checkBroadExtractedSNV },{ "checkDkfzEmblExtractedSNV" , checkDkfzEmblExtractedSNV },
					{ "sangerWorkflow", Pipeline.sanger.toString() }, { "broadWorkflow", Pipeline.broad.toString() },
					{ "dkfzEmblWorkflow", Pipeline.dkfz_embl.toString() }, { "museWorkflow", Pipeline.muse.toString() },
					{ "tumourID", tumourAliquotID }, { "aliquotID", this.normalAliquotID }, { "oxoQScore", this.oxoQScore }, 
					{ "pathToTumour", pathToTumour }, { "normalBamGnosID", this.normalBamGnosID }, { "normalBAMFileName", this.normalBAMFileName } ,
					{ "broadGnosID", this.broadGnosID }, { "sangerGnosID", this.sangerGnosID }, { "dkfzemblGnosID", this.dkfzemblGnosID }, { "museGnosID", this.museGnosID },
					{ "sangerSNVName", sangerSNV}, { "broadSNVName", broadSNV }, { "dkfzEmblSNVName", dkfzEmblSNV }, { "museSNVName", museSNV },
					{ "pathToResults", pathToResults}, { "pathToUploadDir", pathToUploadDir }
				} ).collect(this.collectToMap), "runOxoGFilter.template");
			runOxoGWorkflow.setCommand("( "+runOxoGCommand+" ) || "+ moveToFailed);
		}
		for (Job parent : parents)
		{
			runOxoGWorkflow.addParent(parent);
		}

		
		Function<String,String> changeToOxoGSuffix = (s) ->  pathToUploadDir + s.replace(".vcf.gz", ".oxoG.vcf.gz");
		Function<String,String> changeToOxoGTBISuffix = changeToOxoGSuffix.andThen((s) -> s+=".tbi");
		
		BiConsumer<String,Function<String,String>> addToFilesForUpload = (vcfName, vcfNameProcessor) -> 
		{
			if (this.allowMissingFiles)
			{
				if (vcfName!=null && vcfName.trim().length()>0)
				{
					filesForUpload.add(vcfNameProcessor.apply(vcfName));
				}
			}
			else
			{
				filesForUpload.add(vcfNameProcessor.apply(vcfName));
			}
		};
		//regular VCFs
		addToFilesForUpload.accept(broadSNV, changeToOxoGSuffix);
		addToFilesForUpload.accept(dkfzEmblSNV, changeToOxoGSuffix);
		addToFilesForUpload.accept(sangerSNV, changeToOxoGSuffix);
		addToFilesForUpload.accept(museSNV, changeToOxoGSuffix);
		//index files
		addToFilesForUpload.accept(broadSNV, changeToOxoGTBISuffix);
		addToFilesForUpload.accept(dkfzEmblSNV, changeToOxoGTBISuffix);
		addToFilesForUpload.accept(sangerSNV, changeToOxoGTBISuffix);
		addToFilesForUpload.accept(museSNV, changeToOxoGTBISuffix);
		//Extracted SNVs
		addToFilesForUpload.accept(extractedSangerSNV, getFileName.andThen(changeToOxoGSuffix));
		addToFilesForUpload.accept(extractedBroadSNV, getFileName.andThen(changeToOxoGSuffix));
		addToFilesForUpload.accept(extractedDkfzEmblSNV, getFileName.andThen(changeToOxoGSuffix));
		//index files
		addToFilesForUpload.accept(extractedSangerSNV, getFileName.andThen(changeToOxoGTBISuffix));
		addToFilesForUpload.accept(extractedBroadSNV, getFileName.andThen(changeToOxoGTBISuffix));
		addToFilesForUpload.accept(extractedDkfzEmblSNV, getFileName.andThen(changeToOxoGTBISuffix));
		
		filesForUpload.add("/datastore/files_for_upload/" + this.normalAliquotID + ".gnos_files_tumour_" + tumourAliquotID + ".tar");
		filesForUpload.add("/datastore/files_for_upload/" + this.normalAliquotID + ".call_stats_tumour_" + tumourAliquotID + ".txt.gz.tar");
		
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
			String snvVcf = mergedVcfs.stream().filter(isSnv).findFirst().get().getFileName(); 
			String svVcf = mergedVcfs.stream().filter(isSv).findFirst().get().getFileName();
			String indelVcf = mergedVcfs.stream().filter(isIndel).findFirst().get().getFileName();
			String command = TemplateUtils.getRenderedTemplate(Arrays.stream( new String[][] {
				{ "containerNameSuffix", bamType + (bamType == BAMType.tumour ? "_with_tumour_"+tumourID:"") },
				{ "minibamName", minibamName+".bam"},  {"snvPadding", String.valueOf(this.snvPadding)}, {"svPadding", String.valueOf(this.svPadding)},
				{ "indelPadding", String.valueOf(this.indelPadding) }, { "pathToBam", bamPath },
				{ "snvVcf", snvVcf }, { "svVcf", svVcf }, { "indelVcf", indelVcf }
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
			String broadOxogSNVFileName = this.filesForUpload.stream().filter(p -> ((p.contains(tumourAliquotID) && p.contains("broad-mutect") && p.endsWith(passFilteredOxoGSuffix)))).findFirst().orElseGet(emptyStringWhenMissingFilesAllowed);
			String broadOxoGSNVFromIndelFileName = this.filesForUpload.stream().filter(p -> (p.contains(Pipeline.broad.toString()) && isExtractedSNV.test(p) )).findFirst().orElseGet(emptyStringWhenMissingFilesAllowed);
			
			
			String sangerOxogSNVFileName = this.filesForUpload.stream().filter(p -> ((p.contains(tumourAliquotID) && p.contains("svcp_") && p.endsWith(passFilteredOxoGSuffix)))).findFirst().orElseGet(emptyStringWhenMissingFilesAllowed);
			String sangerOxoGSNVFromIndelFileName = this.filesForUpload.stream().filter(p -> (p.contains(Pipeline.sanger.toString()) && isExtractedSNV.test(p) )).findFirst().orElseGet(emptyStringWhenMissingFilesAllowed);
			
			String dkfzEmbleOxogSNVFileName = this.filesForUpload.stream().filter(p -> ((p.contains(tumourAliquotID) && p.contains("dkfz-snvCalling") && p.endsWith(passFilteredOxoGSuffix)))).findFirst().orElseGet(emptyStringWhenMissingFilesAllowed);
			String dkfzEmblOxoGSNVFromIndelFileName = this.filesForUpload.stream().filter(p -> (p.contains(Pipeline.dkfz_embl.toString()) && isExtractedSNV.test(p) )).findFirst().orElseGet(emptyStringWhenMissingFilesAllowed);

			//Remember: MUSE files do not get PASS-filtered. Also, there is no INDEL so there cannot be any SNVs extracted from INDELs.
			String museOxogSNVFileName = this.filesForUpload.stream().filter(p -> p.toUpperCase().contains("MUSE") && p.endsWith(".oxoG.vcf.gz")).findFirst().orElseGet(emptyStringWhenMissingFilesAllowed);
			
			String normalizedBroadIndel = this.normalizedIndels.stream().filter(isBroad.and(matchesTumour(tumourAliquotID))).findFirst().orElse(new VcfInfo()).getFileName();
			String normalizedSangerIndel = this.normalizedIndels.stream().filter(isSanger.and(matchesTumour(tumourAliquotID))).findFirst().orElse(new VcfInfo()).getFileName();
			String normalizedDkfzEmblIndel = this.normalizedIndels.stream().filter(isDkfzEmbl.and(matchesTumour(tumourAliquotID))).findFirst().orElse(new VcfInfo()).getFileName();

			if (!this.allowMissingFiles)
			{
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
			else
			{
				List<Job> indelJobs = new ArrayList<Job>(3);
				List<Job> snvJobs = new ArrayList<Job>(3);
				List<Job> snvFromIndelJobs = new ArrayList<Job>(3);
				
				QuadConsumer<Pipeline, String, String, List<Job>> runAnnotatorIfPossible = (pipeline,vcfName,type,jobs) -> {
					if (vcfName!=null && vcfName.trim().length()>0)
					{
						Job[] parentOrParents = getParentOrParents(jobs, parents);
						Job j = this.runAnnotator(type, pipeline, vcfName, tInf.getTumourMinibamPath(),this.normalMinibamPath, tInf.getAliquotID(), parentOrParents);
						jobs.add(j);
					}
				};
				
				runAnnotatorIfPossible.accept(Pipeline.broad, normalizedBroadIndel,"indel",indelJobs);
				runAnnotatorIfPossible.accept(Pipeline.dkfz_embl, normalizedDkfzEmblIndel,"indel",indelJobs);
				runAnnotatorIfPossible.accept(Pipeline.sanger, normalizedSangerIndel,"indel",indelJobs);
				
				runAnnotatorIfPossible.accept(Pipeline.broad, broadOxogSNVFileName,"SNV",snvJobs);
				runAnnotatorIfPossible.accept(Pipeline.dkfz_embl, dkfzEmbleOxogSNVFileName,"SNV",snvJobs);
				runAnnotatorIfPossible.accept(Pipeline.sanger, sangerOxogSNVFileName,"SNV",snvJobs);
				runAnnotatorIfPossible.accept(Pipeline.muse, museOxogSNVFileName,"SNV",snvJobs);
				
				runAnnotatorIfPossible.accept(Pipeline.broad, broadOxoGSNVFromIndelFileName,"SNV",snvFromIndelJobs);
				runAnnotatorIfPossible.accept(Pipeline.dkfz_embl, dkfzEmblOxoGSNVFromIndelFileName,"SNV",snvFromIndelJobs);
				runAnnotatorIfPossible.accept(Pipeline.sanger, sangerOxoGSNVFromIndelFileName,"SNV",snvFromIndelJobs);
				
				finalAnnotatorJobs.addAll(indelJobs);
				finalAnnotatorJobs.addAll(snvJobs);
				finalAnnotatorJobs.addAll(snvFromIndelJobs);

			}
		}
		return finalAnnotatorJobs;
	}

	/**
	 * If jobs is not empty, return the last item in that list. If it IS empty, return parents.
	 * @param jobs
	 * @param parents
	 * @return
	 */
	private Job[] getParentOrParents(List<Job> jobs, Job... parents) {  
		return jobs.size()>0 ? Arrays.asList(jobs.get(jobs.size()-1)).toArray(new Job[1]) : parents;
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
			
			Job installTools = this.installTools(copy);
			
			// indicate job is in downloading stage.
			String pathToScripts = this.getWorkflowBaseDir() + "/scripts";
			Job move2download = GitUtils.gitMove("queued-jobs", "downloading-jobs", this.getWorkflow(), this.JSONlocation, this.JSONrepoName, this.JSONfolderName, this.GITname, this.GITemail, this.gitMoveTestMode, this.JSONfileName, pathToScripts ,installTools, pullRepo);
			Job move2running;
			if (!skipDownload) {
				//Download jobs. VCFs downloading serial. Trying to download all in parallel seems to put too great a strain on the system 
				//since the icgc-storage-client can make full use of all cores on a multi-core system. 
				DownloadMethod downloadMethod = DownloadMethod.valueOf(this.downloadMethod);
				
				Function<Predicate<VcfInfo>, List<String>> buildVcfListByPredicate = (p) -> Stream.concat(
						this.vcfs.stream().filter(p).map(m -> m.getObjectID()), 
						this.vcfs.stream().filter(p).map(m -> m.getIndexObjectID())
					).collect(Collectors.toList()) ; 
				
				List<String> sangerList =  buildVcfListByPredicate.apply(isSanger);
				List<String> broadList = buildVcfListByPredicate.apply(isBroad);
				List<String> dkfzEmblList = buildVcfListByPredicate.apply(isDkfzEmbl);
				List<String> museList = buildVcfListByPredicate.apply(isMuse);
				List<String> normalList = Arrays.asList( this.bamNormalIndexObjectID,this.bamNormalObjectID);
				//System.out.println("DEBUG: sangerList: "+sangerList.toString());
				Map<String,List<String>> workflowObjectIDs = new HashMap<String,List<String>>(6);
				workflowObjectIDs.put(Pipeline.broad.toString(), broadList);
				workflowObjectIDs.put(Pipeline.sanger.toString(), sangerList);
				workflowObjectIDs.put(Pipeline.dkfz_embl.toString(), dkfzEmblList);
				workflowObjectIDs.put(Pipeline.muse.toString(), museList);
				workflowObjectIDs.put(BAMType.normal.toString(), normalList);
				for (int i = 0; i < this.tumours.size(); i ++)
				{
					TumourInfo tInfo = this.tumours.get(i);
					List<String> tumourIDs = new ArrayList<String>();
					tumourIDs.add(tInfo.getBamTumourIndexObjectID());
					tumourIDs.add(tInfo.getBamTumourObjectID());
					workflowObjectIDs.put(BAMType.tumour+"_"+tInfo.getAliquotID(), tumourIDs);
				}
				
				Map<String,String> workflowURLs = new HashMap<String,String>(6);
				workflowURLs.put(Pipeline.broad.toString(), this.broadGNOSRepoURL);
				workflowURLs.put(Pipeline.sanger.toString(), this.sangerGNOSRepoURL);
				workflowURLs.put(Pipeline.dkfz_embl.toString(), this.dkfzEmblGNOSRepoURL);
				workflowURLs.put(Pipeline.muse.toString(), this.museGNOSRepoURL);
				workflowURLs.put(BAMType.normal.toString(), this.normalBamGNOSRepoURL);
				for (int i = 0 ; i < this.tumours.size(); i++)
				{
					workflowURLs.put(BAMType.tumour.toString()+"_"+tumours.get(i).getAliquotID(), tumours.get(i).getTumourBamGNOSRepoURL());
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
				//...except for MUSE filenames.
				if (vInfo.getOriginatingPipeline()!=Pipeline.muse)
				{
					vInfo.setFileName(addPassFilteredSuffix.apply( vInfo.getFileName() ) );
				}
			}
			
			// OxoG will run after move2running. Move2running will run after all the jobs that perform input file downloads and file preprocessing have finished.
			
			List<Job> preprocessIndelsJobs = new ArrayList<Job>(this.tumours.size() * 3);
			for (int i =0; i < this.tumours.size(); i++)
			{
				String tumourAliquotID = tumours.get(i).getAliquotID();
				final String vcfNotFoundToken = "VCF_NOT_FOUND";
				BiFunction<String, Predicate<VcfInfo>, String> generateVcfName = (s,p) ->"/"+ s +"/"+ this.vcfs.stream()
																								.filter(vcfMatchesTypePipelineTumour(isIndel, p, tumourAliquotID))
																								.map(m -> m.getFileName())
																								.findFirst().orElse(vcfNotFoundToken);

				String sangerIndelVcfName = generateVcfName.apply(this.sangerGnosID, isSanger);
				if (!sangerIndelVcfName.endsWith(vcfNotFoundToken))
				{
					Job sangerPreprocessVCF = this.preProcessIndelVCF(sangerPassFilter, Pipeline.sanger, sangerIndelVcfName, tumourAliquotID);
					preprocessIndelsJobs.add(sangerPreprocessVCF);
				}
				
				String dkfzEmblIndelVcfName = generateVcfName.apply(this.dkfzemblGnosID, isDkfzEmbl);
				if (!dkfzEmblIndelVcfName.endsWith(vcfNotFoundToken))
				{
					Job dkfzEmblPreprocessVCF = this.preProcessIndelVCF(dkfzemblPassFilter, Pipeline.dkfz_embl, dkfzEmblIndelVcfName, tumourAliquotID);
					preprocessIndelsJobs.add(dkfzEmblPreprocessVCF);
				}
				
				String broadIndelVcfName = generateVcfName.apply(this.broadGnosID, isBroad);
				if (!broadIndelVcfName.endsWith(vcfNotFoundToken))
				{
					Job broadPreprocessVCF = this.preProcessIndelVCF(broadPassFilter, Pipeline.broad, broadIndelVcfName, tumourAliquotID);
					preprocessIndelsJobs.add(broadPreprocessVCF);
				}
			}
			List<Job> combineVCFJobs = new ArrayList<Job>(this.tumours.size());
			for (int i =0; i< this.tumours.size(); i++)
			{	
				Job combineVCFsByType = this.combineVCFsByType(this.tumours.get(i).getAliquotID(), preprocessIndelsJobs.toArray(new Job[preprocessIndelsJobs.size()]) );
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
				}
				else
				{
					oxoG = this.doOxoG(tInf.getTumourBamGnosID()+"/"+tInf.getTumourBAMFileName(),tInf.getAliquotID());
				}
				for (int j =0 ; j<combineVCFJobs.size(); j++)
				{
					oxoG.addParent(combineVCFJobs.get(j));
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
