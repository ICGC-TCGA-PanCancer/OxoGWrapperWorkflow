package com.github.seqware;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.github.seqware.downloaders.DownloaderBuilder;
import com.github.seqware.downloaders.GNOSDownloader;
import com.github.seqware.downloaders.ICGCStorageDownloader;
import com.github.seqware.downloaders.S3Downloader;
import com.github.seqware.jobgenerators.OxoGJobGenerator;
import com.github.seqware.jobgenerators.PcawgAnnotatorJobGenerator;
import com.github.seqware.jobgenerators.PreprocessJobGenerator;
import com.github.seqware.jobgenerators.UploadJobGenerator;
import com.github.seqware.jobgenerators.VariantBamJobGenerator;
import com.github.seqware.jobgenerators.VariantBamJobGenerator.UpdateBamForUpload;

import net.sourceforge.seqware.pipeline.workflowV2.model.Job;

public class OxoGWrapperWorkflow extends BaseOxoGWrapperWorkflow {

	Consumer<String> updateFilesForUpload = (s) -> this.filesForUpload.add(s);

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
	public enum VCFType{
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
	public enum Pipeline {
		sanger, dkfz_embl, broad, muse
	}
	
	/**
	 * Defines what BAM types there are:
	 * <ul><li>normal</li><li>tumour</li></ul>
	 * @author sshorser
	 *
	 */
	public enum BAMType{
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
		PreprocessJobGenerator generator = new PreprocessJobGenerator(this.JSONlocation, this.JSONrepoName, this.JSONfolderName, this.JSONfileName);
		passFilter = generator.passFilterWorkflow(this, workflowName, parents);
		
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
		PreprocessJobGenerator generator = new PreprocessJobGenerator(this.JSONlocation, this.JSONrepo, this.JSONfolderName, this.JSONfileName);
		generator.setTumourAliquotID(tumourAliquotID);
		Consumer<VcfInfo> updateExtractedSNVs = (v) -> this.extractedSnvsFromIndels.add(v);
		Consumer<VcfInfo> updateNormalizedINDELs = (v) -> this.normalizedIndels.add(v);
		Job preProcess = generator.preProcessIndelVCF(this, parent, workflowName, vcfName, tumourAliquotID, this.updateFilesForUpload, updateExtractedSNVs, updateNormalizedINDELs);
		
		return preProcess;
	}
	
	/**
	 * This will combine VCFs from different workflows by the same type. All INDELs will be combined into a new output file,
	 * all SVs will be combined into a new file, all SNVs will be combined into a new file. 
	 * @param parents
	 * @return
	 */
	private Job combineVCFsByType(String tumourAliquotID, Job ... parents)
	{
		
		PreprocessJobGenerator generator = new PreprocessJobGenerator(this.JSONlocation, this.JSONrepo, this.JSONfolderName, this.JSONfileName);

		List<VcfInfo> nonIndels =this.vcfs.stream().filter(p -> p.getOriginatingTumourAliquotID().equals(tumourAliquotID) && isIndel.negate().test(p)).collect(Collectors.toList());
		List<VcfInfo> indels = this.normalizedIndels.stream().filter(p -> p.getOriginatingTumourAliquotID().equals(tumourAliquotID)).collect(Collectors.toList());
		Consumer<VcfInfo> updateMergedVCFs = (v) -> this.mergedVcfs.add(v);
		Job vcfCombineJob = generator.combineVCFsByType(this, nonIndels, indels ,updateMergedVCFs, parents);

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
		Predicate<? super VcfInfo> isSangerSNV = this.vcfMatchesTypePipelineTumour(isSnv, isSanger, tumourAliquotID);
		Predicate<? super VcfInfo> isBroadSNV = this.vcfMatchesTypePipelineTumour(isSnv, isBroad, tumourAliquotID);
		Predicate<? super VcfInfo> isDkfzEmblSNV = this.vcfMatchesTypePipelineTumour(isSnv, isDkfzEmbl, tumourAliquotID);
		Predicate<? super VcfInfo> isMuseSNV = this.vcfMatchesTypePipelineTumour(isSnv, isMuse, tumourAliquotID);
		
		OxoGJobGenerator oxogJobGenerator = new OxoGJobGenerator(this.JSONlocation, this.JSONrepoName, this.JSONfolderName, this.JSONfileName, tumourAliquotID, this.normalAliquotID);
		oxogJobGenerator.setBroadGnosID(this.broadGnosID);
		oxogJobGenerator.setSangerSNV(this.getVcfName(isSangerSNV,this.vcfs));
		oxogJobGenerator.setBroadSNV(this.getVcfName(isBroadSNV,this.vcfs));
		oxogJobGenerator.setDkfzEmblSNV(this.getVcfName(isDkfzEmblSNV,this.vcfs));
		oxogJobGenerator.setMuseSNV(this.getVcfName(isMuseSNV,this.vcfs));
		oxogJobGenerator.setExtractedSangerSNV(this.getVcfName(isSangerSNV,this.extractedSnvsFromIndels));
		oxogJobGenerator.setExtractedBroadSNV(this.getVcfName(isBroadSNV,this.extractedSnvsFromIndels));
		oxogJobGenerator.setExtractedDkfzEmblSNV(this.getVcfName(isDkfzEmblSNV,this.extractedSnvsFromIndels));
		oxogJobGenerator.setGitMoveTestMode(this.gitMoveTestMode);
		
		Consumer<String> updateFilesToUpload = (s) -> this.filesForUpload.add(s);
		Job runOxoGWorkflow = this.getWorkflow().createBashJob("run OxoG Filter for tumour "+tumourAliquotID);
		if (!this.skipOxoG)
		{
			runOxoGWorkflow = oxogJobGenerator.doOxoG(this, pathToTumour, updateFilesToUpload , parents);
		}
		
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

		
		if (!this.skipVariantBam)
		{
			VariantBamJobGenerator variantBamJobGenerator = new VariantBamJobGenerator(this.JSONlocation, this.JSONrepoName, this.JSONfolderName, this.JSONfileName);
			variantBamJobGenerator.setGitMoveTestMode(this.gitMoveTestMode);
			variantBamJobGenerator.setIndelPadding(String.valueOf(this.indelPadding));
			variantBamJobGenerator.setSnvPadding(String.valueOf(this.snvPadding));
			variantBamJobGenerator.setSvPadding(String.valueOf(this.svPadding));
			variantBamJobGenerator.setGitMoveTestMode(this.gitMoveTestMode);
			variantBamJobGenerator.setTumourAliquotID(tumourID);
			variantBamJobGenerator.setSnvVcf(mergedVcfs.stream().filter(isSnv).findFirst().get().getFileName());
			variantBamJobGenerator.setSvVcf(mergedVcfs.stream().filter(isSv).findFirst().get().getFileName());
			variantBamJobGenerator.setIndelVcf(mergedVcfs.stream().filter(isIndel).findFirst().get().getFileName());
			
			UpdateBamForUpload<String, String> updateFilesForUpload = (path, id) -> {
				if (id==null || id.trim().equals(""))
				{
					this.normalMinibamPath = path;
					this.filesForUpload.add(path);
				}
				else
				{
					for (TumourInfo tInfo : this.tumours)
					{
						if (tInfo.getAliquotID().equals(id))
						{
							tInfo.setTumourMinibamPath(path);
							filesForUpload.add(path);
						}
					}
				}
			};
			
			String bamName = ( bamType == BAMType.normal ? this.normalBAMFileName : tumourBAMFileName);
			runVariantbam = variantBamJobGenerator.doVariantBam(this, bamType, bamName, bamPath, tumourBAMFileName, tumourID, updateFilesForUpload, parents);
		}
		return runVariantbam;
	}
	
	/**
	 * Uploads files. Will use the vcf-upload script in pancancer/pancancer_upload_download:1.7 to generate metadata.xml, analysis.xml, and the GTO file, and
	 * then rsync everything to a staging server. 
	 * @param parentJob
	 * @return
	 */
	private Job doUpload(Job parentJob) {
		
		UploadJobGenerator generator = new UploadJobGenerator();
		generator.setJSONFileInfo(this.JSONlocation, this.JSONrepoName, this.JSONfolderName, this.JSONfileName);
		generator.setChangelogURL(this.changelogURL);
		generator.setDonorID(this.donorID);
		generator.setGitMoveTestMode(this.gitMoveTestMode);
		generator.setGnosKey(this.gnosKey);
		generator.setGnosMetadataUploadURL(this.gnosMetadataUploadURL);
		generator.setIndelPadding(String.valueOf(this.indelPadding));
		generator.setNormalMetdataURL(this.normalMetdataURL);
		generator.setOxoQScore(this.oxoQScore);
		generator.setSnvPadding(String.valueOf(this.snvPadding));
		generator.setSpecimenID(this.specimenID);
		generator.setStudyRefNameOverride(this.studyRefNameOverride);
		generator.setSvPadding(String.valueOf(svPadding));
		generator.setTumourMetadataURLs(  String.join("," , this.tumours.stream().map(t -> t.getTumourMetdataURL()).collect(Collectors.toList())) );
		generator.setUploadKey(this.uploadKey);
		generator.setUploadURL(this.uploadURL);
		generator.setWorkflowSourceURL(this.workflowSourceURL);
		generator.setWorkflowURL(this.workflowURL);
		
		List<String> vcfsForUpload = this.filesForUpload.stream().filter(p -> ((p.contains(".vcf") || p.endsWith(".tar")) && !( p.contains("SNVs_from_INDELs") || p.contains("extracted-snv"))) ).distinct().collect(Collectors.toList());
		List<String> bamFilesForUpload = this.filesForUpload.stream().filter( p -> p.contains(".bam") || p.contains(".bai") ).distinct().collect(Collectors.toList());
		return generator.doUpload(this, parentJob, bamFilesForUpload, vcfsForUpload);
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
		List<Job> finalAnnotatorJobs = new ArrayList<Job>();
		
		Predicate<String> isExtractedSNV = p -> p.contains("extracted-snv") && p.endsWith(".vcf.gz");
		final String passFilteredOxoGSuffix = ".pass-filtered.oxoG.vcf.gz";
		//list filtering should only ever produce one result.
		
		for (int i = 0; i < this.tumours.size(); i++)
		{
			TumourInfo tInf = this.tumours.get(i);
			String tumourAliquotID = tInf.getAliquotID();
			PcawgAnnotatorJobGenerator generator = new PcawgAnnotatorJobGenerator(this.JSONlocation, this.JSONrepoName, this.JSONfolderName, this.JSONfileName);
			generator.setGitMoveTestMode(this.gitMoveTestMode);
			generator.setBroadOxogSNVFileName(this.filesForUpload.stream().filter(p -> ((p.contains(tumourAliquotID) && p.contains("broad-mutect") && p.endsWith(passFilteredOxoGSuffix)))).findFirst().orElseGet(emptyStringWhenMissingFilesAllowed));
			generator.setBroadOxoGSNVFromIndelFileName(this.filesForUpload.stream().filter(p -> (p.contains(Pipeline.broad.toString()) && isExtractedSNV.test(p) )).findFirst().orElseGet(emptyStringWhenMissingFilesAllowed));
			generator.setSangerOxogSNVFileName(this.filesForUpload.stream().filter(p -> ((p.contains(tumourAliquotID) && p.contains("svcp_") && p.endsWith(passFilteredOxoGSuffix)))).findFirst().orElseGet(emptyStringWhenMissingFilesAllowed));
			generator.setSangerOxoGSNVFromIndelFileName(this.filesForUpload.stream().filter(p -> (p.contains(Pipeline.sanger.toString()) && isExtractedSNV.test(p) )).findFirst().orElseGet(emptyStringWhenMissingFilesAllowed));
			generator.setDkfzEmbleOxogSNVFileName(this.filesForUpload.stream().filter(p -> ((p.contains(tumourAliquotID) && p.contains("dkfz-snvCalling") && p.endsWith(passFilteredOxoGSuffix)))).findFirst().orElseGet(emptyStringWhenMissingFilesAllowed));
			generator.setDkfzEmblOxoGSNVFromIndelFileName(this.filesForUpload.stream().filter(p -> (p.contains(Pipeline.dkfz_embl.toString()) && isExtractedSNV.test(p) )).findFirst().orElseGet(emptyStringWhenMissingFilesAllowed));

			//Remember: MUSE files do not get PASS-filtered. Also, there is no INDEL so there cannot be any SNVs extracted from INDELs.
			generator.setMuseOxogSNVFileName(this.filesForUpload.stream().filter(p -> p.toUpperCase().contains("MUSE") && p.endsWith(".oxoG.vcf.gz")).findFirst().orElseGet(emptyStringWhenMissingFilesAllowed));
			generator.setNormalizedBroadIndel(this.normalizedIndels.stream().filter(isBroad.and(matchesTumour(tumourAliquotID))).findFirst().get().getFileName());
			generator.setNormalizedDkfzEmblIndel(this.normalizedIndels.stream().filter(isDkfzEmbl.and(matchesTumour(tumourAliquotID))).findFirst().get().getFileName());
			generator.setNormalizedSangerIndel(this.normalizedIndels.stream().filter(isSanger.and(matchesTumour(tumourAliquotID))).findFirst().get().getFileName());
			
			List<Job> jobs = generator.doAnnotations(this, tInf.getAliquotID(), tInf.getTumourMinibamPath(), this.normalMinibamPath, this.updateFilesForUpload, parents);
			finalAnnotatorJobs.addAll(jobs);
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
				for (int i = 0; i < this.tumours.size() ; i ++)
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
					Job downloadTumourBamParent;
					if (i==0)
					{
						downloadTumourBamParent = downloadNormalBam;
					}
					else
					{
						downloadTumourBamParent = getTumourJobs.get(i-1);
					}
					downloadTumourBam = this.getBAM(downloadTumourBamParent, downloadMethod, BAMType.tumour,chooseObjects.apply( BAMType.tumour.toString()+"_"+this.tumours.get(i).getAliquotID() ) );
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
			// ...No, we're not going to filter the Muse SNV file.
			

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
					Job sangerPreprocessVCF = this.preProcessIndelVCF(sangerPassFilter, Pipeline.sanger,sangerIndelVcfName, this.tumours.get(i).getAliquotID());
					preprocessIndelsJobs.add(sangerPreprocessVCF);
				}
				String dkfzEmblIndelVcfName = generateVcfName.apply(this.dkfzemblGnosID, isDkfzEmbl);
				if (!dkfzEmblIndelVcfName.endsWith(vcfNotFoundToken))
				{
					Job dkfzEmblPreprocessVCF = this.preProcessIndelVCF(dkfzemblPassFilter, Pipeline.dkfz_embl, dkfzEmblIndelVcfName, this.tumours.get(i).getAliquotID());
					preprocessIndelsJobs.add(dkfzEmblPreprocessVCF);
				}
				String broadIndelVcfName = generateVcfName.apply(this.broadGnosID, isBroad);
				if (broadIndelVcfName.endsWith(vcfNotFoundToken))
				{
					Job broadPreprocessVCF = this.preProcessIndelVCF(broadPassFilter, Pipeline.broad, broadIndelVcfName, this.tumours.get(i).getAliquotID());
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
