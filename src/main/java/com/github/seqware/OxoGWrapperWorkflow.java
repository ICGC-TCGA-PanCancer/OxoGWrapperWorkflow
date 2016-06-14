package com.github.seqware;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.github.seqware.jobgenerators.DownloadJobGenerator;
import com.github.seqware.jobgenerators.OxoGJobGenerator;
import com.github.seqware.jobgenerators.PcawgAnnotatorJobGenerator;
import com.github.seqware.jobgenerators.PreprocessJobGenerator;
import com.github.seqware.jobgenerators.UploadJobGenerator;
import com.github.seqware.jobgenerators.VariantBamJobGenerator;
import com.github.seqware.jobgenerators.VariantBamJobGenerator.UpdateBamForUpload;

import net.sourceforge.seqware.pipeline.workflowV2.model.Job;

public class OxoGWrapperWorkflow extends BaseOxoGWrapperWorkflow {

	Consumer<String> updateFilesForUpload = (s) -> this.filesForUpload.add(s);

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
		sanger, dkfz_embl, broad, muse, smufin
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
		gtdownload, icgcStorageClient, s3, filesystemCopy
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
		Job copy = this.getWorkflow().createBashJob("copy /home/ubuntu/.gnos");
		copy.setCommand("mkdir /datastore/credentials && cp -r /home/ubuntu/.gnos/* /datastore/credentials && ls -l /datastore/credentials");
		copy.addParent(parentJob);
		
		if (this.vcfDownloadMethod.equals(DownloadMethod.s3.toString()))
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
	 * Perform filtering on all VCF files for a given workflow.
	 * Filtering involves removing lines that are not "PASS" or "."
	 * Output files will have ".pass-filtered." in their name.
	 * @param workflowName The workflow to PASS filter
	 * @param parents List of parent jobs.
	 * @return
	 */
	private Job passFilterWorkflow(Pipeline workflowName, Job ... parents)
	{
		//Job passFilter = this.getWorkflow().createBashJob("pass filter "+workflowName);
		PreprocessJobGenerator generator = new PreprocessJobGenerator(this.JSONlocation, this.JSONrepoName, this.JSONfolderName, this.JSONfileName);

		Job passFilter = generator.passFilterWorkflow(this, workflowName, parents);
		
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
		Job preProcess = generator.preProcessIndelVCF(this, parent, workflowName, vcfName, this.refFile, this.updateFilesForUpload, updateExtractedSNVs, updateNormalizedINDELs);
		
		return preProcess;
	}
	
	/**
	 * Combine vcfs by from from ALL tumours. This is needed when generating the minibam for Normals BAMs in a multi-tumour scenario.  
	 * @param parents
	 * @return
	 */
	private Job combineVCFsByType(Job ... parents)
	{
		PreprocessJobGenerator generator = new PreprocessJobGenerator(this.JSONlocation, this.JSONrepo, this.JSONfolderName, this.JSONfileName);
		List<VcfInfo> nonIndels =this.vcfs.stream().filter(p -> isIndel.negate().test(p)).collect(Collectors.toList());
		List<VcfInfo> indels = this.normalizedIndels.stream().collect(Collectors.toList());
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
		Predicate<? super VcfInfo> isSangerSNV = this.vcfMatchesTypePipelineTumour(isSnv, CommonPredicates.isSanger, tumourAliquotID);
		Predicate<? super VcfInfo> isBroadSNV = this.vcfMatchesTypePipelineTumour(isSnv, CommonPredicates.isBroad, tumourAliquotID);
		Predicate<? super VcfInfo> isDkfzEmblSNV = this.vcfMatchesTypePipelineTumour(isSnv, CommonPredicates.isDkfzEmbl, tumourAliquotID);
		Predicate<? super VcfInfo> isMuseSNV = this.vcfMatchesTypePipelineTumour(isSnv, CommonPredicates.isMuse, tumourAliquotID);

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
		
		OxoGJobGenerator oxogJobGenerator = new OxoGJobGenerator(this.JSONlocation, this.JSONrepoName, this.JSONfolderName, this.JSONfileName, tumourAliquotID, this.normalAliquotID);
		oxogJobGenerator.setSangerSNV(applyPrefixForOxoG.apply(this.getVcfName(isSangerSNV,this.vcfs),Pipeline.sanger.toString()));
		oxogJobGenerator.setBroadSNV(applyPrefixForOxoG.apply(this.getVcfName(isBroadSNV,this.vcfs),Pipeline.broad.toString()));
		oxogJobGenerator.setDkfzEmblSNV(applyPrefixForOxoG.apply(this.getVcfName(isDkfzEmblSNV,this.vcfs),Pipeline.dkfz_embl.toString()));
		oxogJobGenerator.setMuseSNV(applyPrefixForOxoG.apply(this.getVcfName(isMuseSNV,this.vcfs),Pipeline.muse.toString()));
		oxogJobGenerator.setExtractedSangerSNV(this.getVcfName(isSangerSNV,this.extractedSnvsFromIndels));
		oxogJobGenerator.setExtractedBroadSNV(this.getVcfName(isBroadSNV,this.extractedSnvsFromIndels));
		oxogJobGenerator.setExtractedDkfzEmblSNV(this.getVcfName(isDkfzEmblSNV,this.extractedSnvsFromIndels));
		oxogJobGenerator.setGitMoveTestMode(this.gitMoveTestMode);
		oxogJobGenerator.setOxoQScore(this.oxoQScore);
		oxogJobGenerator.setNormalBamGnosID(this.normalBamGnosID);
		oxogJobGenerator.setNormalBAMFileName(this.normalBAMFileName);
		oxogJobGenerator.setBroadGnosID(this.broadGnosID);
		oxogJobGenerator.setSangerGnosID(this.sangerGnosID);
		oxogJobGenerator.setDkfzemblGnosID(this.dkfzemblGnosID);
		oxogJobGenerator.setMuseGnosID(this.museGnosID);
		oxogJobGenerator.setAllowMissingFiles(this.allowMissingFiles);
		
		Consumer<String> updateFilesToUpload = (s) -> this.filesForUpload.add(s);
		Job runOxoGWorkflow;
		if (!this.skipOxoG)
		{
			runOxoGWorkflow = oxogJobGenerator.doOxoG(this, pathToTumour, updateFilesToUpload , parents);
		}
		else
		{
			runOxoGWorkflow = this.getWorkflow().createBashJob("run OxoG Filter for tumour "+tumourAliquotID);
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
		Job runVariantbam;
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
					//only update the normalMinibamPath with the path to the actual BAM.
					//If you get a .bai file here, add it to filesForUpload,
					//but don't do anything else.
					if (path.endsWith(".bam"))
					{
						this.normalMinibamPath = path;
					}
					this.filesForUpload.add(path);
				}
				else
				{
					for (TumourInfo tInfo : this.tumours)
					{
						if (tInfo.getAliquotID().equals(id))
						{
							//Set the tumour minibam path only to the BAM file.
							//If you get a .bai file here, add it to filesForUpload,
							//but don't do anything else.
							if (path.endsWith(".bam"))
							{
								tInfo.setTumourMinibamPath(path);
							}
							filesForUpload.add(path);
						}
					}
				}
			};
			
			String bamName = ( bamType == BAMType.normal ? this.normalBAMFileName : tumourBAMFileName);
			runVariantbam = variantBamJobGenerator.doVariantBam(this, bamType, bamName, bamPath, tumourBAMFileName, tumourID, updateFilesForUpload, parents);
		}
		else
		{
			runVariantbam = this.getWorkflow().createBashJob("run "+bamType+(bamType==BAMType.tumour?"_"+tumourID+"_":"")+" variantbam");
			Arrays.stream(parents).forEach(parent -> runVariantbam.addParent(parent));
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
			generator.setAllowMissingFiles(this.allowMissingFiles);
			generator.setBroadOxogSNVFileName(this.filesForUpload.stream().filter(p -> ((p.contains(tumourAliquotID) && p.contains("broad-mutect") && p.endsWith(passFilteredOxoGSuffix)))).findFirst().orElseGet(emptyStringWhenMissingFilesAllowed));
			generator.setBroadOxoGSNVFromIndelFileName(this.filesForUpload.stream().filter(p -> (p.contains(Pipeline.broad.toString()) && isExtractedSNV.test(p) )).findFirst().orElseGet(emptyStringWhenMissingFilesAllowed));
			generator.setSangerOxogSNVFileName(this.filesForUpload.stream().filter(p -> ((p.contains(tumourAliquotID) && p.contains("svcp_") && p.endsWith(passFilteredOxoGSuffix)))).findFirst().orElseGet(emptyStringWhenMissingFilesAllowed));
			generator.setSangerOxoGSNVFromIndelFileName(this.filesForUpload.stream().filter(p -> (p.contains(Pipeline.sanger.toString()) && isExtractedSNV.test(p) )).findFirst().orElseGet(emptyStringWhenMissingFilesAllowed));
			generator.setDkfzEmbleOxogSNVFileName(this.filesForUpload.stream().filter(p -> ((p.contains(tumourAliquotID) && p.contains("dkfz-snvCalling") && p.endsWith(passFilteredOxoGSuffix)))).findFirst().orElseGet(emptyStringWhenMissingFilesAllowed));
			generator.setDkfzEmblOxoGSNVFromIndelFileName(this.filesForUpload.stream().filter(p -> (p.contains(Pipeline.dkfz_embl.toString()) && isExtractedSNV.test(p) )).findFirst().orElseGet(emptyStringWhenMissingFilesAllowed));

			//Remember: MUSE files do not get PASS-filtered. Also, there is no INDEL so there cannot be any SNVs extracted from INDELs.
			generator.setMuseOxogSNVFileName(this.filesForUpload.stream().filter(p -> p.toUpperCase().contains("MUSE") && p.endsWith(".oxoG.vcf.gz")).findFirst().orElseGet(emptyStringWhenMissingFilesAllowed));
			generator.setNormalizedBroadIndel(this.normalizedIndels.stream().filter(CommonPredicates.isBroad.and(matchesTumour(tumourAliquotID))).findFirst().orElse(new VcfInfo()).getFileName());
			generator.setNormalizedDkfzEmblIndel(this.normalizedIndels.stream().filter(CommonPredicates.isDkfzEmbl.and(matchesTumour(tumourAliquotID))).findFirst().orElse(new VcfInfo()).getFileName());
			generator.setNormalizedSangerIndel(this.normalizedIndels.stream().filter(CommonPredicates.isSanger.and(matchesTumour(tumourAliquotID))).findFirst().orElse(new VcfInfo()).getFileName());
			
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
			//Remember: smufin won't have a gnos ID so don't try to use that in the path for stat.
			statFilesCMD+="stat /datastore/vcf/"+vcfInfo.getOriginatingPipeline().toString()+"/"
							+(vcfInfo.getOriginatingPipeline()!=Pipeline.smufin?vcfInfo.getPipelineGnosID()+"/":"")
							+vcfInfo.getFileName()+ " && \\\n";
			statFilesCMD+="stat /datastore/vcf/"+vcfInfo.getOriginatingPipeline().toString()+"/"
							+(vcfInfo.getOriginatingPipeline()!=Pipeline.smufin?vcfInfo.getPipelineGnosID()+"/":"")
							+vcfInfo.getIndexFileName()+ " && \\\n";
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
				move2running = doDownload(pathToScripts, move2download);
			}
			else {
				// If user is skipping download, then we will just move directly to runnning...
				move2running = GitUtils.gitMove("downloading-jobs", "running-jobs", this.getWorkflow(), this.JSONlocation, this.JSONrepoName, this.JSONfolderName, this.GITname, this.GITemail, this.gitMoveTestMode, this.JSONfileName , pathToScripts,move2download);
			}

			Job statFiles = statInputFiles(move2running);
			
			Job sangerPassFilter = this.passFilterWorkflow(Pipeline.sanger, statFiles);
			Job broadPassFilter = this.passFilterWorkflow(Pipeline.broad, statFiles);
			Job dkfzemblPassFilter = this.passFilterWorkflow(Pipeline.dkfz_embl, statFiles);
			Job smufinPassFilter = this.passFilterWorkflow(Pipeline.smufin, statFiles);
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
				
				String sangerIndelVcfName = generateVcfName.apply(this.sangerGnosID, CommonPredicates.isSanger);
				if (!sangerIndelVcfName.endsWith(vcfNotFoundToken))
				{
					Job sangerPreprocessVCF = this.preProcessIndelVCF(sangerPassFilter, Pipeline.sanger,sangerIndelVcfName, this.tumours.get(i).getAliquotID());
					preprocessIndelsJobs.add(sangerPreprocessVCF);
				}
				String dkfzEmblIndelVcfName = generateVcfName.apply(this.dkfzemblGnosID, CommonPredicates.isDkfzEmbl);
				if (!dkfzEmblIndelVcfName.endsWith(vcfNotFoundToken))
				{
					Job dkfzEmblPreprocessVCF = this.preProcessIndelVCF(dkfzemblPassFilter, Pipeline.dkfz_embl, dkfzEmblIndelVcfName, this.tumours.get(i).getAliquotID());
					preprocessIndelsJobs.add(dkfzEmblPreprocessVCF);
				}
				String broadIndelVcfName = generateVcfName.apply(this.broadGnosID, CommonPredicates.isBroad);
				if (!broadIndelVcfName.endsWith(vcfNotFoundToken))
				{
					Job broadPreprocessVCF = this.preProcessIndelVCF(broadPassFilter, Pipeline.broad, broadIndelVcfName, this.tumours.get(i).getAliquotID());
					preprocessIndelsJobs.add(broadPreprocessVCF);
				}
				//smufin INDEL VCFs will be in /datastore/vcf/smufin - they will not be nested in a GNOS ID-named directory.
				String smufinIndelVcfName = generateVcfName.apply("", CommonPredicates.isSmufin);
				if (!smufinIndelVcfName.endsWith(vcfNotFoundToken))
				{
					Job smufinPreprocessVCF = this.preProcessIndelVCF(smufinPassFilter, Pipeline.smufin, broadIndelVcfName, this.tumours.get(i).getAliquotID());
					preprocessIndelsJobs.add(smufinPreprocessVCF);
				}
			}
			//TODO: This probably doesn't need to be a list anymore.
			List<Job> combineVCFJobs = new ArrayList<Job>(this.tumours.size());
			Job combineVCFJob = this.combineVCFsByType(preprocessIndelsJobs.toArray(new Job[preprocessIndelsJobs.size()]));
			combineVCFJobs.add(combineVCFJob);
			
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

			Job minibamSanityCheck = this.getWorkflow().createBashJob("Check minibams");
			String moveToFailed = GitUtils.gitMoveCommand("running-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, this.getWorkflowBaseDir() + "/scripts/");
			minibamSanityCheck.setCommand("(bash "+pathToScripts+ "/check_minibams.sh) || "+moveToFailed);
			variantBamJobs.stream().forEach(job -> minibamSanityCheck.addParent(job));
			parentJobsToAnnotationJobs.add(minibamSanityCheck);

			//set up parent jobs to annotation jobs
			oxogJobs.stream().forEach(job -> parentJobsToAnnotationJobs.add(job));
			List<Job> annotationJobs = new ArrayList<Job>();
			if (!this.skipAnnotation)
			{
				annotationJobs = this.doAnnotations( parentJobsToAnnotationJobs.toArray(new Job[parentJobsToAnnotationJobs.size()]));
			}
			
			//Now do the Upload. The parents jobs are the Annotation jobs, unless the user set skipAnnotations=true, so there will
			//not be any annotation jobs. In that case Upload's parents are the jobs that would have been parents to Annotation.
			Job[] parentsToUpload = (annotationJobs !=null && annotationJobs.size()>0)
										? annotationJobs.toArray(new Job[annotationJobs.size()])
										: parentJobsToAnnotationJobs.toArray(new Job[parentJobsToAnnotationJobs.size()]);
			
			
			if (!skipUpload)
			{
				// indicate job is in uploading stage.
				Job move2uploading = this.gitMove( "running-jobs", "uploading-jobs", parentsToUpload);
				Job uploadResults = doUpload(move2uploading);
				// indicate job is complete.
				this.gitMove( "uploading-jobs", "completed-jobs", uploadResults);
			}
			else
			{
				this.gitMove( "running-jobs", "completed-jobs",parentsToUpload);
			}
			//System.out.println(this.filesForUpload);
		}
		catch (Exception e)
		{
			throw new RuntimeException ("Exception caught: "+e.getMessage(), e);
		}
	}

	private Job doDownload(String pathToScripts, Job parent) throws Exception {
		Job move2running;
		
		DownloadJobGenerator generator = new DownloadJobGenerator();
		generator.setBamNormalIndexObjectID(this.bamNormalIndexObjectID);
		generator.setBamNormalObjectID(this.bamNormalObjectID);
		generator.setBroadGNOSRepoURL(this.broadGNOSRepoURL);
		generator.setDkfzEmblGNOSRepoURL(this.dkfzEmblGNOSRepoURL);
		generator.setGITemail(this.GITemail);
		generator.setGitMoveTestMode(this.gitMoveTestMode);
		generator.setGITname(this.GITname);
		generator.setGtDownloadBamKey(this.gtDownloadBamKey);
		generator.setGtDownloadVcfKey(this.gtDownloadVcfKey);
		generator.setJSONFileInfo(this.JSONlocation, this.JSONrepoName, this.JSONfolderName, this.JSONfileName);
		generator.setMuseGNOSRepoURL(this.museGNOSRepoURL);
		generator.setNormalBamGNOSRepoURL(this.normalBamGNOSRepoURL);
		generator.setObjectToFilenames(this.objectToFilenames);
		generator.setSangerGNOSRepoURL(this.sangerGNOSRepoURL);
		generator.setStorageSource(this.storageSource);
		generator.setTumours(this.tumours);
		generator.setVcfs(this.vcfs);
		generator.setWorkflowNamestoGnosIds(this.workflowNamestoGnosIds);
		//System.out.println(this.pipelineDownloadMethods);
		generator.setPipelineDownloadMethods(this.pipelineDownloadMethods);
		generator.setFileSystemSourceDir(this.fileSystemSourcePath);
		generator.setBamDownloadMethod(DownloadMethod.valueOf(this.bamDownloadMethod));
		
		move2running = generator.doDownload(this, pathToScripts, parent);
		return move2running;
	}
}
