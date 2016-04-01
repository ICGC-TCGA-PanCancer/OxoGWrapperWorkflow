package com.github.seqware;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.github.seqware.downloaders.DownloaderBuilder;
import com.github.seqware.downloaders.GNOSDownloader;
import com.github.seqware.downloaders.ICGCStorageDownloader;
import com.github.seqware.downloaders.S3Downloader;

import net.sourceforge.seqware.pipeline.workflowV2.model.Job;



public class OxoGWrapperWorkflow extends BaseOxoGWrapperWorkflow {

	public enum DownloadMethod
	{
		gtdownload, icgcStorageClient, s3
	}
	
	private static final String DESCRIPTION_END = " Note the 'ANALYSIS_TYPE' is 'REFERENCE_ASSEMBLY' but a better term to describe this analysis is 'SEQUENCE_VARIATION' as defined by the EGA's SRA 1.5 schema."
			+ " Please note the reference used for alignment was hs37d, see ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/reference/phase2_reference_assembly_sequence/README_human_reference_20110707 for more information."
			+ " Briefly this is the integrated reference sequence from the GRCh37 primary assembly (chromosomal plus unlocalized and unplaced contigs),"
			+ " the rCRS mitochondrial sequence (AC:NC_012920), Human herpesvirus 4 type 1 (AC:NC_007605) and the concatenated decoy sequences (hs37d5cs.fa.gz)."
			+ " Variant calls may not be present for all contigs in this reference.";

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
		passFilter.setCommand("sudo chmod a+rw -R /datastore/vcf/ \n( for f in $(ls /datastore/vcf/"+workflowName+"/*/*.vcf.gz | grep -v pass | tr '\\n' ' ' ) ; do \n"
							+ "    echo \"processing $f\" \n"
							+ "    bgzip -d -c $f | grep -Po \"^#.*$|([^\t]*\t){6}(PASS\t|\\.\t).*\" > ${f/.vcf.gz/}.pass-filtered.vcf \n"
							+ "    bgzip -f ${f/.vcf.gz/}.pass-filtered.vcf \n"
							+ "    #bgzip -d -c $f | grep -Pv \"^#.*$|([^\t]*\t){6}(PASS\t|\\.\t).*\" > ${f/.vcf.gz/}.non-pass-filtered.vcf \n"
							+ "    #bgzip -f ${f/.vcf.gz/}.non-pass-filtered.vcf \n"
							+ "done) || "+moveToFailed);
		
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
		String runBCFToolsNormCommand = "sudo chmod a+rw -R /datastore/vcf/ && ( docker run --rm --name normalize_indel_"+workflowName+" "
					+ " -v "+outDir+"/"+vcfName+":/datastore/datafile.vcf.gz "
					+ " -v "+outDir+"/"+":/outdir/:rw "
					+ " -v /refdata/:/ref/"
					+ " compbio/ngseasy-base:a1.0-002 /bin/bash -c \""
						+ " bgzip -d -c /datastore/datafile.vcf.gz \\\n"
						//The goal is a sed call that looks like this, where the "t" is a tab...
						//sed -e 's/t$/t./g' -e 's/tt/t.t/g'  -e 's/\([^t]\)tt\([^t]\)/\1t.t\2/g' -e 's/tt/t.t/g' -e -e 's/^Mt/MTt/g' -e 's/\\(##.*\\);$/\\1/g'
						//First, replace a tab at the end of a line with a tab and a dot because the dot was miossing.
						//Second, replace any two tabs next to each other with tab-dot-tab because the dot was missing in between them.
						//Third, replace any two tabs that are still beside each other and are book-ended by non-tabs with
						//the original leading/trailing characters and two tabs with a dot in between. 
						//Fourth, replace any remaining sequential tabs with tab-dot-tab.
						//Fifth, replace any leading M with MT
						//Sixth, get rid of trailing semi-colons in header lines.
						+ " | sed -e s/"+sedTab+"$/"+sedTab+"./g"
							  + " -e s/"+sedTab+sedTab+"/"+sedTab+"."+sedTab+"/g"
							  + " -e s/\\([^"+sedTab+"]\\)"+sedTab+sedTab+"\\([^"+sedTab+"]\\)/\\1"+sedTab+"."+sedTab+"\\2/g"
							  + " -e s/"+sedTab+sedTab+"/"+sedTab+"."+sedTab+"/g"
							  + " -e 's/^M\\([[:blank:]]\\)/MT\\1/g'"
							  + " -e 's/\\(##.*\\);$/\\1/g' \\\n"
						+ " > /outdir/"+fixedIndel+" && \\\n"
						+ " bcftools norm -c w -m -any -Oz -f /ref/"+this.refFile+"  /outdir/"+fixedIndel+" "  
						+ " > /outdir/"+normalizedINDELName
						+ " && bgzip -f /outdir/"+fixedIndel
						+ " && tabix -f -p vcf /outdir/"+fixedIndel+".gz "
						+ " && tabix -f -p vcf /outdir/"+normalizedINDELName + "\" ) ";
		
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
		Job runOxoGWorkflow = this.getWorkflow().createBashJob("run OxoG Filter");
		String moveToFailed = GitUtils.gitMoveCommand("running-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, this.getWorkflowBaseDir() + "/scripts/");
		Function<String,String> getFileName = (s) -> {  return s.substring(s.lastIndexOf("/")); };
		String extractedSnvCheck = "EXTRACTED_SNV_MOUNT=\"\"\n"
									+ "EXTRACTED_SNV_FILES=\"\"\n"
									+ "if (( $(zcat "+this.sangerExtractedSNVVCFName+" | grep \"^[^#]\" | wc -l) > 0 )) ; then \n"
									+ "    echo \""+this.sangerExtractedSNVVCFName+" has SNVs.\"\n"
									+ "    EXTRACTED_SNV_MOUNT=\"${EXTRACTED_SNV_MOUNT} -v "+this.sangerExtractedSNVVCFName+":/datafiles/VCF/"+Pipeline.sanger+"/"+getFileName.apply(this.sangerExtractedSNVVCFName)+" \"\n"
									+ "    EXTRACTED_SNV_FILES=\"${EXTRACTED_SNV_FILES} /datafiles/VCF/"+Pipeline.sanger+"/"+getFileName.apply(this.sangerExtractedSNVVCFName)+" \"\n"
									+ "fi\n  "
									+ "if (( $(zcat "+this.broadExtractedSNVVCFName+" | grep \"^[^#]\" | wc -l) > 0 )) ; then \n"
									+ "    echo \""+this.broadExtractedSNVVCFName+" has SNVs.\"\n"
									+ "    EXTRACTED_SNV_MOUNT=\"${EXTRACTED_SNV_MOUNT} -v "+this.broadExtractedSNVVCFName+":/datafiles/VCF/"+Pipeline.broad+"/"+getFileName.apply(this.broadExtractedSNVVCFName)+" \"\n"
									+ "    EXTRACTED_SNV_FILES=\"${EXTRACTED_SNV_FILES} /datafiles/VCF/"+Pipeline.broad+"/"+getFileName.apply(this.broadExtractedSNVVCFName)+" \"\n"
									+ "fi\n "
									+ "if (( $(zcat "+this.dkfzEmblExtractedSNVVCFName+" | grep \"^[^#]\" | wc -l) > 0 )) ; then \n"
									+ "    echo \""+this.dkfzEmblExtractedSNVVCFName+" has SNVs.\"\n"
									+ "    EXTRACTED_SNV_MOUNT=\"${EXTRACTED_SNV_MOUNT} -v "+this.dkfzEmblExtractedSNVVCFName+":/datafiles/VCF/"+Pipeline.dkfz_embl+"/"+getFileName.apply(this.dkfzEmblExtractedSNVVCFName)+" \"\n"
									+ "    EXTRACTED_SNV_FILES=\"${EXTRACTED_SNV_FILES} /datafiles/VCF/"+Pipeline.dkfz_embl+"/"+getFileName.apply(this.dkfzEmblExtractedSNVVCFName)+" \"\n"
									+ "fi\n ";
		if (!skipOxoG)
		{
			String oxogMounts = " -v /refdata/:/cga/fh/pcawg_pipeline/refdata/ \\\n"
					+ " -v /datastore/oxog_workspace/tumour_"+tumourID+"/:/cga/fh/pcawg_pipeline/jobResults_pipette/jobs/"+this.aliquotID+"/:rw \\\n" 
					+ " -v /datastore/bam/:/datafiles/BAM/ \\\n"
					+ " -v /datastore/vcf/"+Pipeline.broad+"/"+this.broadGnosID+"/"+"/:/datafiles/VCF/"+Pipeline.broad+"/ \\\n"
					+ " -v /datastore/vcf/"+Pipeline.sanger+"/"+this.sangerGnosID+"/"+"/:/datafiles/VCF/"+Pipeline.sanger+"/ \\\n"
					+ " -v /datastore/vcf/"+Pipeline.dkfz_embl+"/"+this.dkfzemblGnosID+"/"+"/:/datafiles/VCF/"+Pipeline.dkfz_embl+"/ \\\n"
					+ " -v /datastore/vcf/"+Pipeline.muse+"/"+this.museGnosID+"/"+"/:/datafiles/VCF/"+Pipeline.muse+"/ \\\n"
					+ " ${EXTRACTED_SNV_MOUNT} \\\n"
					+ " -v /datastore/oxog_results/tumour_"+tumourID+"/:/cga/fh/pcawg_pipeline/jobResults_pipette/results:rw \\\n";
			
			String oxogCommand = "/cga/fh/pcawg_pipeline/pipelines/run_one_pipeline.bash pcawg /cga/fh/pcawg_pipeline/pipelines/oxog_pipeline.py \\\n"
					+ this.aliquotID + " \\\n"
					//genereate a string of the tumours' gnos IDs and file names.
					//Can the OxoG filter handle more than 1 tumour at a time?
					+ " /datafiles/BAM/tumour/"+pathToTumour+" \\\n" 
					+ " /datafiles/BAM/normal/" +this.normalBamGnosID + "/" +  this.normalBAMFileName + " \\\n" 
					+ " " + this.oxoQScore + " \\\n"
					+ " /datafiles/VCF/"+Pipeline.sanger+"/" + this.sangerSNVName + " \\\n"
					+ " /datafiles/VCF/"+Pipeline.dkfz_embl+"/" + this.dkfzEmblSNVName  + " \\\n"
					+ " /datafiles/VCF/"+Pipeline.muse+"/" + this.museSNVName + " \\\n"
					+ " /datafiles/VCF/"+Pipeline.broad+"/" + this.broadSNVName 
					+ " ${EXTRACTED_SNV_FILES} " ;
			runOxoGWorkflow.setCommand("(("+extractedSnvCheck+"\nset -x;\ndocker run --rm --name=\"oxog_filter\" "+oxogMounts+" oxog /bin/bash -c \"" + oxogCommand+ "\" ;\nset +x;) || echo \"OxoG Exit Code: $?\"  ) || "+moveToFailed);
			
			
		}
		for (Job parent : parents)
		{
			runOxoGWorkflow.addParent(parent);
		}
		Job extractOutputFiles = this.getWorkflow().createBashJob("extract oxog output files from tar");
		extractOutputFiles.setCommand("(cd /datastore/oxog_results && sudo chmod a+rw -R /datastore/oxog_results/ && tar -xvkf ./"+this.aliquotID+".gnos_files.tar  ) || "+moveToFailed);
		extractOutputFiles.addParent(runOxoGWorkflow);
		String pathToResults = "/datastore/oxog_results/tumour_"+tumourID+"/cga/fh/pcawg_pipeline/jobResults_pipette/jobs/"+this.aliquotID+"/links_for_gnos/annotate_failed_sites_to_vcfs/";
		String pathToUploadDir = "/datastore/files_for_upload/";
		
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
		prepOxoGTarAndMutectCallsforUpload.setCommand("( ([ -d /datastore/files_for_upload ] || mkdir -p /datastore/files_for_upload) "
				+ " && cp /datastore/oxog_results/tumour_"+tumourID+"/"+this.aliquotID+".gnos_files.tar /datastore/files_for_upload/tumour_"+tumourID+"/ \\\n"
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
				+ " && cp /datastore/oxog_workspace/tumour_"+tumourID+"/mutect/sg/gather/"+this.aliquotID+".call_stats.txt /datastore/files_for_upload/tumour_"+tumourID+"/"+this.aliquotID+".call_stats.txt \\\n"
				+ " && cd /datastore/files_for_upload/tumour_"+tumourID+"/ && gzip -f "+this.aliquotID+".call_stats.txt && tar -cvf ./"+this.aliquotID+".call_stats.txt.gz.tar ./"+this.aliquotID+".call_stats.txt.gz ) || "+moveToFailed);
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
	private Job doVariantBam(Job parent, BAMType bamType, String bamPath, String tumourBAMFileName, String tumourMinibamPath) {
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
			this.filesForUpload.add(tumourMinibamPath);
			this.filesForUpload.add(tumourMinibamPath+".bai");
		}
		
		if (!this.skipVariantBam)
		{
			String command = DockerCommandCreator.createVariantBamCommand(bamType, minibamName+".bam", bamPath, this.snvVCF, this.svVCF, this.indelVCF, this.svPadding, this.snvPadding, this.indelPadding);
			
			// There will be two jobs trying to run variantbam at the same time and I think sometimes, they both check for the existence of /datastore/variantbam_results AT THE EXACT SAME TIME 
			// so they both try to create it, then one fails and has to retry. But this messes up the process of moving job files to failed-jobs because variantbak will succeed on retry BUT the 
			// job file will still be in the failed-jobs directory because there's no easy way to move it out.
			// Ugly workaround: if the bam type is NORMAL, sleep for a few seconds. That should let the tumour variantbam job get far enough ahead to create the directory properly.
			String sleep = bamType == BAMType.normal ? " sleep 10 && " : "" ;
			command = sleep + "(( [ -d /datastore/variantbam_results/ ] || mkdir /datastore/variantbam_results ) && sudo chmod a+rw -R /datastore/variantbam_results/ && " + command + " ) ";
			
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
		// Will need to run gtupload to generate the analysis.xml and manifest files (but not actually upload). 
		// The tar file contains all results.
		Job generateAnalysisFilesVCFs = this.getWorkflow().createBashJob("generate_analysis_files_for_VCF_upload");
		String moveToFailed = GitUtils.gitMoveCommand("uploading-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, this.getWorkflowBaseDir() + "/scripts/");
		
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
		
		String vcfDescription="These are the OxoG-filtered (with an OxoQ Score of "+this.oxoQScore+") and Annotated VCFs for specimen "+this.specimenID+" from donor "+this.donorID+","
				+ " based on the VCFs produced from the core variant calling workflows."
				+ " The results consist of one or more VCF files plus optional tar.gz files that contain additional file types."
				+ " This uses the "+this.getName()+" workflow, version "+this.getVersion()+" available at "+this.workflowURL+"."
				+ " This workflow can be created from source, see "+this.workflowSourceURL+"."
				+ " For a complete change log see "+this.changelogURL+"."
				+ OxoGWrapperWorkflow.DESCRIPTION_END;
		
		//This ugliness is here because of the OxoG results on SNVs from INDELs. We won't know until the workflow actually runs if there are any SNVs from INDELs.
		//So we need to build up the list of files to upload using a bash script that will be evaluated at runtime rather
		//than Java code that gets evaluated when the workflow is built.
		generateAnalysisFilesVCFCommand += "\n\nSNV_FROM_INDEL_OXOG=\'\'\n"
										+ "SNV_FROM_INDEL_OXOG_INDEX=\'\'\n"
										+ "SNV_FROM_INDEL_OXOG_MD5=\'\'\n"
										+ "SNV_FROM_INDEL_OXOG_INDEX_MD5=\'\'\n"
										+ "for f in $(ls /datastore/files_for_upload/ | grep -e from_INDELs -e extracted | grep -e gz | grep -v md5) ; do \n"
										+ "    echo \"processing $f\"\n"
										+ "    f=/datastore/files_for_upload/$f \n"
										+ "    md5sum $f | cut -d ' ' -f 1 > $f.md5 \n"
										+ "    if [[ \"$f\" =~ tbi|idx ]] ; then \n"
										+ "        SNV_FROM_INDEL_OXOG_INDEX=$SNV_FROM_INDEL_OXOG_INDEX,$f\n"
										+ "        SNV_FROM_INDEL_OXOG_INDEX_MD5=$SNV_FROM_INDEL_OXOG_INDEX_MD5,$f.md5\n"
										+ "    else \n"
										+ "        SNV_FROM_INDEL_OXOG=$SNV_FROM_INDEL_OXOG,$f\n"
										+ "        SNV_FROM_INDEL_OXOG_MD5=$SNV_FROM_INDEL_OXOG_MD5,$f.md5\n"
										+ "    fi\n"
										+ "done\n\n"
										+ "echo \"SNV_FROM_INDEL_OXOG_INDEX = $SNV_FROM_INDEL_OXOG_INDEX\" \n"
										+ "echo \"SNV_FROM_INDEL_OXOG_INDEX_MD5 = $SNV_FROM_INDEL_OXOG_INDEX_MD5\" \n"
										+ "echo \"SNV_FROM_INDEL_OXOG = $SNV_FROM_INDEL_OXOG\" \n"
										+ "echo \"SNV_FROM_INDEL_OXOG_MD5 = $SNV_FROM_INDEL_OXOG_MD5\" \n\n";
		generateAnalysisFilesVCFCommand += "\nset -x\n\n docker run --rm --name=upload_vcfs_and_tarballs -v /datastore/vcf-upload-prep/:/vcf/ -v "+this.gnosKey+":/gnos.key -v /datastore/:/datastore/ "
				+ " pancancer/pancancer_upload_download:1.7 /bin/bash -c \" cat << DESCRIPTIONFILE > /vcf/description.txt\n"
				+ vcfDescription
				+ "\nDESCRIPTIONFILE\n"
				+ " perl -I /opt/gt-download-upload-wrapper/gt-download-upload-wrapper-2.0.13/lib/ /opt/vcf-uploader/vcf-uploader-2.0.9/gnos_upload_vcf.pl \\\n"
					+ " --gto-only --key /gnos.key --upload-url "+this.gnosMetadataUploadURL+" "
					+ " --metadata-urls "+this.normalMetdataURL+","+this.tumours.stream().map(t -> t.getTumourMetdataURL()).reduce("", (a,b)->a+=b+"," )+" \\\n"
					+ " --vcfs "+vcfs+"$SNV_FROM_INDEL_OXOG \\\n"
					+ " --tarballs "+tars+"$SNV_FROM_INDEL_TARBALL \\\n"
					+ " --tarball-md5sum-files "+tarMD5Sums+"$SNV_FROM_INDEL_TARBALL_MD5 \\\n"
					+ " --vcf-idxs "+vcfIndicies+"$SNV_FROM_INDEL_OXOG_INDEX \\\n"
					+ " --vcf-md5sum-files "+vcfMD5Sums+"$SNV_FROM_INDEL_OXOG_MD5 \\\n"
					+ " --vcf-idx-md5sum-files "+vcfIndexMD5Sums+"$SNV_FROM_INDEL_OXOG_INDEX_MD5 \\\n"
					+ " --workflow-name OxoGWorkflow-OxoGFiltering \\\n"
					+ " --study-refname-override "+this.studyRefNameOverride + " \\\n"
					+ " --description-file /vcf/description.txt \\\n"
					+ " --workflow-version " + this.getVersion() + " \\\n"
					+ " --workflow-src-url https://github.com/ICGC-TCGA-PanCancer/OxoGWrapperWorkflow --workflow-url https://github.com/ICGC-TCGA-PanCancer/OxoGWrapperWorkflow  \"\nset +x\n";
		
		
		
		//copy the analaysis.xml, manifest.xml *.gto files to /datastore/files_for_upload
		generateAnalysisFilesVCFCommand += "cp /datastore/vcf-upload-prep/*/*/manifest.xml /datastore/files_for_upload/manifest.xml "
															+ " && cp /datastore/vcf-upload-prep/*/*/analysis.xml /datastore/files_for_upload/analysis.xml "
															+ " && cp /datastore/vcf-upload-prep/*/*/*.gto /datastore/files_for_upload/\n";
		generateAnalysisFilesVCFs.setCommand("( "+generateAnalysisFilesVCFCommand+ " ) || "+moveToFailed);
		generateAnalysisFilesVCFs.addParent(parentJob);
		
		//get the UUID that was submitted with the metadata
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
		
		String bamDescription="These are minibams created for donor "+this.donorID+" by extracing from WG BAMs reads around variants called by any of the core variant calling workflows."
							+ " Specifically, the window sizes are SNV+/-"+this.snvPadding+"bp, indel+/-"+this.indelPadding+"bp, SV+/-"+this.svPadding+"bp."
							+ " The results consist of one or more BAM files plus optional tar.gz files that contain additional file types."
							+ " This uses the "+this.getName()+" workflow, version "+this.getVersion()+" available at "+this.workflowURL+"."
							+ " This workflow can be created from source, see "+this.workflowSourceURL+"."
							+ " For a complete change log see "+this.changelogURL+"."
							+ OxoGWrapperWorkflow.DESCRIPTION_END;
		
		generateAnalysisFilesBAMsCommand += "\n docker run --rm --name=upload_bams -v /datastore/bam-upload-prep/:/vcf/ -v "+this.gnosKey+":/gnos.key -v /datastore/:/datastore/ "
				+ " pancancer/pancancer_upload_download:1.7 /bin/bash -c \"cat << DESCRIPTIONFILE > /vcf/description.txt\n"
				+ bamDescription
				+ "\nDESCRIPTIONFILE\n"
				+ " perl -I /opt/gt-download-upload-wrapper/gt-download-upload-wrapper-2.0.13/lib/ /opt/vcf-uploader/vcf-uploader-2.0.9/gnos_upload_vcf.pl \\\n"
					+ " --gto-only --key /gnos.key --upload-url "+this.gnosMetadataUploadURL+" "
					+ " --metadata-urls "+this.normalMetdataURL+","+this.tumours.stream().map(t -> t.getTumourMetdataURL()).reduce("", (a,b)->a+=b+"," )+" \\\n"
					+ " --bams "+bams+" \\\n"
					+ " --bam-bais "+bamIndicies+" \\\n"
					+ " --bam-md5sum-files "+bamMD5Sums+" \\\n"
					+ " --bam_bai-md5sum-files "+bamIndexMD5Sums+" \\\n"
					+ " --workflow-name OxoGWorkflow-variantbam \\\n"
					+ " --study-refname-override "+this.studyRefNameOverride + " \\\n"
					+ " --description-file /vcf/description.txt \\\n"
					+ " --workflow-version " + this.getVersion() + " \\\n"
					+ " --workflow-src-url https://github.com/ICGC-TCGA-PanCancer/OxoGWrapperWorkflow --workflow-url https://github.com/ICGC-TCGA-PanCancer/OxoGWrapperWorkflow  \"\n";
		
		generateAnalysisFilesBAMsCommand += "\n cp /datastore/bam-upload-prep/*/*/manifest.xml /datastore/variantbam_results/manifest.xml "
											+ " && cp /datastore/bam-upload-prep/*/*/analysis.xml /datastore/variantbam_results/analysis.xml "
											+ " && cp /datastore/bam-upload-prep/*/*/*.gto /datastore/variantbam_results/";
		generateAnalysisFilesBAMs.setCommand("( "+generateAnalysisFilesBAMsCommand+" ) || "+moveToFailed);
		generateAnalysisFilesBAMs.addParent(parentJob);
	
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
		uploadBAMResults.addParent(uploadVCFResults);
		return uploadBAMResults;
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
		String outDir = "/datastore/files_for_upload/";
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
					+ "    bgzip -f -c "+outDir+annotatedFileName+" > "+outDir+"/"+annotatedFileName+".gz \n"
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
//		finalAnnotatorJobs.add(sangerSNVFromIndelAnnotatorJob);
//		finalAnnotatorJobs.add(sangerSNVAnnotatorJob);
//		finalAnnotatorJobs.add(sangerIndelAnnotatorJob);
//		finalAnnotatorJobs.add(museSNVAnnotatorJob);

		return finalAnnotatorJobs;
	}

	private Job statInputFiles(Job parent) {
		Job statFiles = this.getWorkflow().createBashJob("stat downloaded input files");
		String moveToFailed = GitUtils.gitMoveCommand("running-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, this.getWorkflowBaseDir() + "/scripts/");
		String statFilesCMD = "( ";
		statFilesCMD += "stat /datastore/vcf/"+Pipeline.sanger.toString()+"/"+this.sangerGnosID+"/"+this.sangerSNVName + " && \\\n";
		statFilesCMD += "stat /datastore/vcf/"+Pipeline.sanger.toString()+"/"+this.sangerGnosID+"/"+this.sangerSNVIndexFileName + " && \\\n";
		statFilesCMD += "stat /datastore/vcf/"+Pipeline.sanger.toString()+"/"+this.sangerGnosID+"/"+this.sangerSVName + " && \\\n";
		statFilesCMD += "stat /datastore/vcf/"+Pipeline.sanger.toString()+"/"+this.sangerGnosID+"/"+this.sangerSVIndexFileName + " && \\\n";
		statFilesCMD += "stat /datastore/vcf/"+Pipeline.sanger.toString()+"/"+this.sangerGnosID+"/"+this.sangerIndelName + " && \\\n";
		statFilesCMD += "stat /datastore/vcf/"+Pipeline.sanger.toString()+"/"+this.sangerGnosID+"/"+this.sangerINDELIndexFileName + " && \\\n";

		statFilesCMD += "stat /datastore/vcf/"+Pipeline.broad.toString()+"/"+this.broadGnosID+"/"+this.broadSNVName + " && \\\n";
		statFilesCMD += "stat /datastore/vcf/"+Pipeline.broad.toString()+"/"+this.broadGnosID+"/"+this.broadSNVIndexFileName + " && \\\n";
		statFilesCMD += "stat /datastore/vcf/"+Pipeline.broad.toString()+"/"+this.broadGnosID+"/"+this.broadSVName + " && \\\n";
		statFilesCMD += "stat /datastore/vcf/"+Pipeline.broad.toString()+"/"+this.broadGnosID+"/"+this.broadSVIndexFileName + " && \\\n";
		statFilesCMD += "stat /datastore/vcf/"+Pipeline.broad.toString()+"/"+this.broadGnosID+"/"+this.broadIndelName + " && \\\n";
		statFilesCMD += "stat /datastore/vcf/"+Pipeline.broad.toString()+"/"+this.broadGnosID+"/"+this.broadINDELIndexFileName + " && \\\n";

		statFilesCMD += "stat /datastore/vcf/"+Pipeline.dkfz_embl.toString()+"/"+this.dkfzemblGnosID+"/"+this.dkfzEmblSNVName + " && \\\n";
		statFilesCMD += "stat /datastore/vcf/"+Pipeline.dkfz_embl.toString()+"/"+this.dkfzemblGnosID+"/"+this.dkfzEmblSNVIndexFileName + " && \\\n";
		statFilesCMD += "stat /datastore/vcf/"+Pipeline.dkfz_embl.toString()+"/"+this.dkfzemblGnosID+"/"+this.dkfzEmblSVName + " && \\\n";
		statFilesCMD += "stat /datastore/vcf/"+Pipeline.dkfz_embl.toString()+"/"+this.dkfzemblGnosID+"/"+this.dkfzEmblSVIndexFileName + " && \\\n";
		statFilesCMD += "stat /datastore/vcf/"+Pipeline.dkfz_embl.toString()+"/"+this.dkfzemblGnosID+"/"+this.dkfzEmblIndelName + " && \\\n";
		statFilesCMD += "stat /datastore/vcf/"+Pipeline.dkfz_embl.toString()+"/"+this.dkfzemblGnosID+"/"+this.dkfzEmblINDELIndexFileName + " && \\\n";
		
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
				List<String> tumourList = Arrays.asList( this.bamTumourIndexObjectID,this.bamTumourObjectID);
				
				Map<String,List<String>> workflowObjectIDs = new HashMap<String,List<String>>(6);
				workflowObjectIDs.put(Pipeline.broad.toString(), broadList);
				workflowObjectIDs.put(Pipeline.sanger.toString(), sangerList);
				workflowObjectIDs.put(Pipeline.dkfz_embl.toString(), dkfzEmblList);
				workflowObjectIDs.put(Pipeline.muse.toString(), museList);
				workflowObjectIDs.put(BAMType.normal.toString(), normalList);
				workflowObjectIDs.put(BAMType.tumour.toString(), tumourList);
				
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
				for (int i = 0 ; i < this.tumours.size(); i++)
				{	
					Job downloadTumourBam;
					//download the tumours sequentially.
					if (i==0)
					{
						downloadTumourBam = this.getBAM(downloadNormalBam, downloadMethod, BAMType.tumour,chooseObjects.apply( BAMType.tumour.toString() ) );
					}
					else
					{
						downloadTumourBam = this.getBAM(getTumourJobs.get(i-1), downloadMethod, BAMType.tumour,chooseObjects.apply( BAMType.tumour.toString() ) );
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
			for (TumourInfo tInf : this.tumours)
			{
				Job oxoG = this.doOxoG(tInf.getTumourBamGnosID()+"/"+tInf.getTumourBAMFileName(),tInf.getTumourBamGnosID(), combineVCFsByType);
				oxogJobs.add(oxoG);
			}
			
			Job normalVariantBam = this.doVariantBam(combineVCFsByType,BAMType.normal,"/datastore/bam/normal/"+this.normalBamGnosID+"/"+this.normalBAMFileName,this.normalBamGnosID, this.normalMinibamPath);
			List<Job> parentJobsToAnnotationJobs = new ArrayList<Job>(this.tumours.size());

			//create a list of tumour variant-bam jobs.
			List<Job> variantBamJobs = new ArrayList<Job>(this.tumours.size()+1);
			for (int i = 0; i < this.tumours.size() ; i ++)
			{
				Job tumourVariantBam = this.doVariantBam(combineVCFsByType,BAMType.tumour,"/datastore/bam/tumour/"+this.tumours.get(i).getTumourBamGnosID()+"/"+this.tumours.get(i).getTumourBAMFileName(), this.tumours.get(i).getTumourBamGnosID(), this.tumours.get(i).getTumourMinibamPath());
				variantBamJobs.add(tumourVariantBam);
			}
			variantBamJobs.add(normalVariantBam);

			//Now that we've built our list of variantbam and oxog jobs, we can set up the proper parent-child relationships between them.
			//The idea is to run 1 OxoG at the same time as 2 variantbam jobs.
			for (int i=2; i < Math.max(variantBamJobs.size(), variantBamJobs.size()); i+=2)
			{
				variantBamJobs.get(i).addParent(variantBamJobs.get(i-2));
				variantBamJobs.get(i+1).addParent(variantBamJobs.get(i-2));
			}
			for (int i=1; i < Math.max(oxogJobs.size(), oxogJobs.size()); i+=2)
			{
				oxogJobs.get(i).addParent(oxogJobs.get(i-1));
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
