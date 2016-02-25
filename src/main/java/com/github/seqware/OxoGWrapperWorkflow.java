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
import java.util.function.Function;
import java.util.stream.Collectors;

import net.sourceforge.seqware.pipeline.workflowV2.model.Job;

public class OxoGWrapperWorkflow extends BaseOxoGWrapperWorkflow {

	

	/**
	 * Generates a rules file that is used for the variant program that produces minibams.
	 * NOTE: currently injecting the rules inline in the call to variantbam so the rules file won't actually be used...
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
		copy.setCommand("mkdir /datastore/credentials && cp -r ~/.gnos/* /datastore/credentials && ls -l /datastore/credentials");
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
		String storageClientDockerCmdNormal =" docker run --rm --name get_bam_"+bamType+" "
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
		
		String getVCFCommand = " docker run --rm --name get_vcf_"+workflowName+" "
				+ " -e STORAGE_PROFILE="+this.storageSource+" " 
			    + " -v "+outDir+"/logs/:/icgc/icgc-storage-client/logs/:rw "
				+ " -v /datastore/credentials/collab.token:/icgc/icgc-storage-client/conf/application.properties:ro "
			    + " -v "+outDir+"/:/downloads/:rw"
	    		+ " icgc/icgc-storage-client /bin/bash -c \" "+downloadObjects+" \" ";
		
		String moveToFailed = GitUtils.gitMoveCommand("downloading-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, this.getWorkflowBaseDir() + "/scripts/");				 
		getVCFCommand += (" || " + moveToFailed);
		
		getVCFJob.setCommand(getVCFCommand);
		getVCFJob.addParent(parentJob);



		return getVCFJob;
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
		String moveToFailed = GitUtils.gitMoveCommand("downloading-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, this.getWorkflowBaseDir() + "/scripts/");
		//If we were processing MUSE files we would also need to filter for tier* but we're NOT processing MUSE files so 
		//we don't need to worry about that for now.
		passFilter.setCommand("( for f in $(ls /datastore/vcf/"+workflowName+"/*/*.vcf.gz | grep -v pass | tr '\\n' ' ' ) ; do \n"
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
	private Job installTabix(Job parent)
	{
		Job installTabixJob = this.getWorkflow().createBashJob("install tabix and bgzip");
		
		installTabixJob.setCommand("sudo apt-get install tabix");
		
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
		String runBCFToolsNormCommand = "sudo chmod a+rw -R /datastore/vcf/ && ( docker run --rm --name normalize_indel_"+workflowName+" "
					+ " -v "+outDir+"/"+vcfName+":/datastore/datafile.vcf.gz "
					+ " -v "+outDir+"/"+":/outdir/:rw "
					+ " -v /refdata/:/ref/"
					+ " compbio/ngseasy-base:a1.0-002 /bin/bash -c \""
						+ " bgzip -d -c /datastore/datafile.vcf.gz \\\n"
						+ " | sed -e s/\\\"$(echo -e '\\t\\t')\\\"/\\\"$(echo -e '\\t')\\\".\\\"$(echo -e '\\t')\\\"./g -e s/\\\"$(echo -e '\\t')\\\"$/\\\"$(echo -e '\\t')\\\"./g -e 's/\\(##.*\\);$/\\1/g' \\\n"
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
	private Job doOxoG(Job parent) {
		Job runOxoGWorkflow = this.getWorkflow().createBashJob("run OxoG Filter");
		String moveToFailed = GitUtils.gitMoveCommand("running-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, this.getWorkflowBaseDir() + "/scripts/");

		if (!skipOxoG)
		{
			String oxogMounts = " -v /refdata/:/cga/fh/pcawg_pipeline/refdata/ \\\n"
					+ " -v /datastore/oxog_workspace/:/cga/fh/pcawg_pipeline/jobResults_pipette/jobs/"+this.aliquotID+"/:rw \\\n" 
					+ " -v /datastore/bam/:/datafiles/BAM/ \\\n"
					+ " -v /datastore/vcf/"+Pipeline.broad+"/"+this.broadGnosID+"/"+"/:/datafiles/VCF/"+Pipeline.broad+"/ \\\n"
					+ " -v /datastore/vcf/"+Pipeline.sanger+"/"+this.sangerGnosID+"/"+"/:/datafiles/VCF/"+Pipeline.sanger+"/ \\\n"
					+ " -v /datastore/vcf/"+Pipeline.dkfz_embl+"/"+this.dkfzemblGnosID+"/"+"/:/datafiles/VCF/"+Pipeline.dkfz_embl+"/ \\\n"
					+ " -v /datastore/vcf/"+Pipeline.muse+"/"+this.museGnosID+"/"+"/:/datafiles/VCF/"+Pipeline.muse+"/ \\\n"
					+ " -v /datastore/oxog_results/:/cga/fh/pcawg_pipeline/jobResults_pipette/results:rw \\\n";
			String oxogCommand = "/cga/fh/pcawg_pipeline/pipelines/run_one_pipeline.bash pcawg /cga/fh/pcawg_pipeline/pipelines/oxog_pipeline.py \\\n"
					+ this.aliquotID + " \\\n"
					+ " /datafiles/BAM/tumour/" + this.tumourBamGnosID + "/" + this.tumourBAMFileName + " \\\n" 
					+ " /datafiles/BAM/normal/" +this.normalBamGnosID + "/" +  this.normalBAMFileName + " \\\n" 
					+ " " + this.oxoQScore + " \\\n"
					+ " /datafiles/VCF/"+Pipeline.sanger+"/" + this.sangerSNVName + " \\\n"
					+ " /datafiles/VCF/"+Pipeline.dkfz_embl+"/" + this.dkfzEmblSNVName  + " \\\n"
					+ " /datafiles/VCF/"+Pipeline.muse+"/" + this.museSNVName + " \\\n"
					+ " /datafiles/VCF/"+Pipeline.broad+"/" + this.broadSNVName  ;
			runOxoGWorkflow.setCommand("((docker run --rm --name=\"oxog_filter\" "+oxogMounts+" oxog /bin/bash -c \"" + oxogCommand+ "\" ) || echo \"OxoG Exit Code: $?\"  ) || "+moveToFailed);
			
			
		}
		runOxoGWorkflow.addParent(parent);
		Job extractOutputFiles = this.getWorkflow().createBashJob("extract oxog output files from tar");
		extractOutputFiles.setCommand("(cd /datastore/oxog_results && sudo chmod a+rw -R /datastore/oxog_results/ && tar -xvkf ./"+this.aliquotID+".gnos_files.tar  ) || "+moveToFailed);
		extractOutputFiles.addParent(runOxoGWorkflow);
		String pathToResults = "/datastore/oxog_results/cga/fh/pcawg_pipeline/jobResults_pipette/jobs/"+this.aliquotID+"/links_for_gnos/annotate_failed_sites_to_vcfs/";
		String pathToUploadDir = "/datastore/files_for_upload/";
		this.filesForUpload.add(pathToUploadDir + this.broadSNVName.replace(".vcf.gz", ".oxoG.vcf.gz")) ;
		this.filesForUpload.add(pathToUploadDir + this.dkfzEmblSNVName.replace(".vcf.gz",".oxoG.vcf.gz")) ;
		this.filesForUpload.add(pathToUploadDir + this.sangerSNVName.replace(".vcf.gz", ".oxoG.vcf.gz"))  ;
		this.filesForUpload.add(pathToUploadDir + this.museSNVName.replace(".vcf.gz", ".oxoG.vcf.gz"));
		this.filesForUpload.add(pathToUploadDir + this.broadSNVName.replace(".vcf.gz", ".oxoG.vcf.gz.tbi")) ;
		this.filesForUpload.add(pathToUploadDir + this.dkfzEmblSNVName.replace(".vcf.gz",".oxoG.vcf.gz.tbi")) ;
		this.filesForUpload.add(pathToUploadDir + this.sangerSNVName.replace(".vcf.gz", ".oxoG.vcf.gz.tbi"))  ;
		this.filesForUpload.add(pathToUploadDir + this.museSNVName.replace(".vcf.gz", ".oxoG.vcf.gz.tbi"));
		this.filesForUpload.add("/datastore/oxog_results/" + this.aliquotID + ".gnos_files.tar");
		
		Job prepOxoGTarAndMutectCallsforUpload = this.getWorkflow().createBashJob("prepare OxoG tar and mutect calls file for upload");
		prepOxoGTarAndMutectCallsforUpload.setCommand("( ([ -d /datastore/files_for_upload ] || mkdir -p /datastore/files_for_upload) "
				+ " && cp /datastore/oxog_results/"+this.aliquotID+".gnos_files.tar /datastore/files_for_upload/ \\\n"
				+ " && cp " + pathToResults + this.broadSNVName.replace(".vcf.gz", ".oxoG.vcf.gz")+" "+pathToUploadDir+" \\\n"
				+ " && cp " + pathToResults + this.dkfzEmblSNVName.replace(".vcf.gz", ".oxoG.vcf.gz")+" "+pathToUploadDir+" \\\n"
				+ " && cp " + pathToResults + this.sangerSNVName.replace(".vcf.gz", ".oxoG.vcf.gz")+" "+pathToUploadDir+" \\\n"
				+ " && cp " + pathToResults + this.museSNVName.replace(".vcf.gz", ".oxoG.vcf.gz")+" "+pathToUploadDir+" \\\n"
				+ " && cp " + pathToResults + this.broadSNVName.replace(".vcf.gz", ".oxoG.vcf.gz.tbi")+" "+pathToUploadDir+" \\\n"
				+ " && cp " + pathToResults + this.dkfzEmblSNVName.replace(".vcf.gz", ".oxoG.vcf.gz.tbi")+" "+pathToUploadDir+" \\\n"
				+ " && cp " + pathToResults + this.sangerSNVName.replace(".vcf.gz", ".oxoG.vcf.gz.tbi")+" "+pathToUploadDir+" \\\n"
				+ " && cp " + pathToResults + this.museSNVName.replace(".vcf.gz", ".oxoG.vcf.gz.tbi")+" "+pathToUploadDir+" \\\n"
				// Also need to upload normalized INDELs
				+ " && cp " + this.broadNormalizedIndelVCFName+" "+pathToUploadDir+" \\\n"
				+ " && cp " + this.dkfzEmblNormalizedIndelVCFName+" "+pathToUploadDir+" \\\n"
				+ " && cp " + this.sangerNormalizedIndelVCFName+" "+pathToUploadDir+" \\\n"
				+ " && cp " + this.broadNormalizedIndelVCFName+".tbi "+pathToUploadDir+" \\\n"
				+ " && cp " + this.dkfzEmblNormalizedIndelVCFName+".tbi "+pathToUploadDir+" \\\n"
				+ " && cp " + this.sangerNormalizedIndelVCFName+".tbi "+pathToUploadDir+" \\\n"
				+ " && cp /datastore/oxog_workspace/mutect/sg/gather/"+this.aliquotID+".call_stats.txt /datastore/files_for_upload/"+this.aliquotID+".call_stats.txt \\\n"
				+ " && cd /datastore/files_for_upload/ && gzip -f "+this.aliquotID+".call_stats.txt && tar -cvf ./"+this.aliquotID+".call_stats.txt.gz.tar ./"+this.aliquotID+".call_stats.txt.gz ) || "+moveToFailed);
		this.filesForUpload.add("/datastore/files_for_upload/"+this.aliquotID+".call_stats.txt.gz.tar");
		
		prepOxoGTarAndMutectCallsforUpload.addParent(extractOutputFiles);
		return prepOxoGTarAndMutectCallsforUpload;
	}

	/**
	 * This will run the OxoG Filter program on the SNVs that were extracted from the INDELs, if there were any. It's possible that no SNVs will be extracted from any
	 * INDEL files (in fact, I've been told this is the most likely scenario for most donors) in which case nothing will run. See the script scripts/run_oxog_extracted_SNVs.sh
	 * for more details on this.
	 * @param parent
	 * @return
	 */
	private Job doOxoGSnvsFromIndels(Job parent) {
		Job oxoGOnSnvsFromIndels = this.getWorkflow().createBashJob("running OxoG on SNVs from INDELs");
		if (!skipOxoG)
		{
			String vcf1 = this.sangerExtractedSNVVCFName;
			String vcf2 = this.broadExtractedSNVVCFName;
			String vcf3 = this.dkfzEmblExtractedSNVVCFName;

			String extractionCommand = this.getWorkflowBaseDir()+"/scripts/run_oxog_extracted_SNVs.sh "+
					vcf1+" "+vcf2+" "+vcf3+" "+
					" tumour/" + this.tumourBamGnosID + "/" + this.tumourBAMFileName +  
					" normal/" + this.normalBamGnosID + "/" + this.normalBAMFileName + 
					" " + this.aliquotID + " " + 
					this.oxoQScore;
			
			String moveToFailed = GitUtils.gitMoveCommand("running-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, this.getWorkflowBaseDir() + "/scripts/");
			extractionCommand += (" || " + moveToFailed);
			oxoGOnSnvsFromIndels.setCommand(extractionCommand);
		}
		oxoGOnSnvsFromIndels.addParent(parent);
		// At workflow-build time, we won't know if there's any files to upload from this step. So...
		// The script run_oxog_extracted_SNVs.sh will un-tar the tar file if it exists and copy the files to /datastore/files_for_upload
		// and then ... somehow we have to include those (if they exist) in the vcf-upload script. :/
		
		//TODO: update filesToUpload with files under /datastore/files_for_upload/snvs_from_indels/ 
		// /datastore/files_for_upload/snvs_from_indels/
		String pathToUploadDir = "/datastore/files_for_upload/snvs_from_indels/";
		this.filesForUpload.add( this.broadExtractedSNVVCFName.replace("/datastore/vcf/"+Pipeline.broad+"/", pathToUploadDir).replace(".vcf.gz", ".oxoG.vcf.gz"));
		this.filesForUpload.add( this.dkfzEmblExtractedSNVVCFName.replace("/datastore/vcf/"+Pipeline.dkfz_embl+"/", pathToUploadDir).replace(".vcf.gz",".oxoG.vcf.gz"));
		this.filesForUpload.add( this.sangerExtractedSNVVCFName.replace("/datastore/vcf/"+Pipeline.sanger+"/", pathToUploadDir).replace(".vcf.gz", ".oxoG.vcf.gz"));
		this.filesForUpload.add( this.broadExtractedSNVVCFName.replace("/datastore/vcf/"+Pipeline.broad+"/", pathToUploadDir).replace(".vcf.gz", ".oxoG.vcf.gz.tbi"));
		this.filesForUpload.add( this.dkfzEmblExtractedSNVVCFName.replace("/datastore/vcf/"+Pipeline.dkfz_embl+"/", pathToUploadDir).replace(".vcf.gz",".oxoG.vcf.gz.tbi"));
		this.filesForUpload.add( this.sangerExtractedSNVVCFName.replace("/datastore/vcf/"+Pipeline.sanger+"/", pathToUploadDir).replace(".vcf.gz", ".oxoG.vcf.gz.tbi"));
		this.filesForUpload.add(pathToUploadDir + this.aliquotID + ".gnos_files.tar");

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
			minibamName = this.tumourBAMFileName.replace(".bam", "_minibam");
			this.tumourMinibamPath = "/datastore/variantbam_results/"+minibamName+".bam";
			this.filesForUpload.add(this.tumourMinibamPath);
			this.filesForUpload.add(this.tumourMinibamPath+".bai");
		}
		
		
		if (!this.skipVariantBam)
		{
			String command = DockerCommandCreator.createVariantBamCommand(bamType, minibamName+".bam", bamPath, this.snvVCF, this.svVCF, this.indelVCF, this.svPadding, this.snvPadding, this.indelPadding);
			
			command = "(( [ -d /datastore/variantbam_results/ ] || mkdir /datastore/variantbam_results ) && sudo chmod a+rw -R /datastore/variantbam_results/ && " + command + " ) ";//\\\n && ( cp /datastore/variantbam_results/"+minibamName+".bam /datastore/files_for_upload/ && cp /datastore/variantbam_results/"+minibamName+".bam.bai ) \\\n";
			
			String moveToFailed = GitUtils.gitMoveCommand("running-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, this.getWorkflowBaseDir() + "/scripts/");
			command += (" || " + moveToFailed);
			runOxoGWorkflow.setCommand(command);
		}
		//this.filesForUpload.add("/datastore/variantbam_results/"+minibamName+".bam.bai");
		//this.filesForUpload.add("/datastore/variantbam_results/"+minibamName+".bam");
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
		getLog.setCommand(" docker logs oxog_run");
		getLog.addParent(parent);
		return getLog;
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
		//Files to upload:
		//OxoG files
		//minibams
		//other intermediate files?
		
		//generateAnalysisFilesVCFs.getCommand().addArgument("[ -d /datastore/files_for_upload ] || mkdir -p /datastore/files_for_upload ; \n");
		
		//Files need to be copied to the staging directory
		String vcfs = "";
		String vcfIndicies = "";
		String vcfMD5Sums = "";
		String vcfIndexMD5Sums = "";
		
		String tars = "";
		String tarMD5Sums = "";
		
		for (String file : this.filesForUpload.stream().filter(p -> p.contains(".vcf") || p.endsWith(".tar") ).collect(Collectors.toList()) )
		{
			file = file.trim();
			//md5sum test_files/tumour_minibam.bam.bai | cut -d ' ' -f 1 > test_files/tumour_minibam.bai.md5
			generateAnalysisFilesVCFs.getCommand().addArgument("md5sum "+file+" | cut -d ' ' -f 1 > "+file+".md5 ; \n");
			
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
		
		String vcfDescription="These are the OxoG-filtered (with an OxoQ Score of "+this.oxoQScore+") and Annotated VCFs for specimen "+this.specimenID+" from donor "+this.donorID+" from the core variant calling workflows."
				+ " The results consist of one or more VCF files plus optional tar.gz files that contain additional file types."
				+ " This uses the "+this.getName()+" workflow, version "+this.getVersion()+" available at "+this.workflowURL+"."
				+ " This workflow can be created from source, see "+this.workflowSourceURL+"."
				+ " For a complete change log see "+this.changelogURL+"."
				+ " Note the 'ANALYSIS_TYPE' is 'REFERENCE_ASSEMBLY' but a better term to describe this analysis is 'SEQUENCE_VARIATION' as defined by the EGA's SRA 1.5 schema."
				+ " Please note the reference used for alignment was hs37d, see ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/reference/phase2_reference_assembly_sequence/README_human_reference_20110707 for more information."
				+ " Briefly this is the integrated reference sequence from the GRCh37 primary assembly (chromosomal plus unlocalized and unplaced contigs),"
				+ " the rCRS mitochondrial sequence (AC:NC_012920), Human herpesvirus 4 type 1 (AC:NC_007605) and the concatenated decoy sequences (hs37d5cs.fa.gz)."
				+ " Variant calls may not be present for all contigs in this reference.";
		
		//This ugliness is here because of the OxoG results on SNVs from INDELs. We won't know until the workflow actually runs if there are any SNVs from INDELs.
		//So we need to build up the list of files to upload using a bash script that will be evaluated at runtime rather
		//than Java code that gets evaluated when the workflow is built.
		generateAnalysisFilesVCFs.getCommand().addArgument("\nSNV_FROM_INDEL_OXOG=\'\'\n"
															+ "SNV_FROM_INDEL_OXOG_INDEX=\'\'\n"
															+ "SNV_FROM_INDEL_OXOG_MD5=\'\'\n"
															+ "SNV_FROM_INDEL_OXOG_INDEX_MD5=\'\'\n"
															+ "SNV_FROM_INDEL_TARBALL=\'\'\n"
															+ "SNV_FROM_INDEL_TARBALL_MD5=\'\'\n"
															+ "[ -d /datastore/files_for_upload/snvs_from_indels/ ] &&  for f in $(ls /datastore/files_for_upload/snvs_from_indels/) ; do \n"
															+ "    mv $f /datastore/files_for_upload/$f \n"
															+ "    md5sum $f | cud -d ' ' -f 1 > $f.md5 \n"
															+ "    if [[ \"$f\" =~ gnos_files\\.tar ]] ; then \n"
															+ "        SNV_FROM_INDEL_TARBALL=$f,$SNV_FROM_INDEL_TARBALL\n"
															+ "        SNV_FROM_INDEL_TARBALL_MD5=$f.md5,$SNV_FROM_INDEL_TARBALL_MD5\n"
															+ "    elif [[ \"$f\" =~ tbi|idx ]] ; then \n"
															+ "        SNV_FROM_INDEL_OXOG_INDEX=$SNV_FROM_INDEL_OXOG_INDEX,$f\n"
															+ "        SNV_FROM_INDEL_OXOG_INDEX_MD5=$SNV_FROM_INDEL_OXOG_INDEX_MD5,$f.md5\n"
															+ "    else \n"
															+ "        SNV_FROM_INDEL_OXOG=$SNV_FROM_INDEL_OXOG,$f\n"
															+ "        SNV_FROM_INDEL_OXOG_MD5=$SNV_FROM_INDEL_OXOG_MD5,$f.md5\n"
															+ "    fi\n"
															+ "done\n");
		generateAnalysisFilesVCFs.getCommand().addArgument("\n docker run --rm --name=upload_vcfs_and_tarballs -v /datastore/vcf-upload-prep/:/vcf/ -v "+this.gnosKey+":/gnos.key -v /datastore/:/datastore/ "
				+ " pancancer/pancancer_upload_download:1.7 /bin/bash -c \" cat << DESCRIPTIONFILE > /vcf/description.txt\n"
				+ vcfDescription
				+ "\nDESCRIPTIONFILE"
				+ " perl -I /opt/gt-download-upload-wrapper/gt-download-upload-wrapper-2.0.13/lib/ /opt/vcf-uploader/vcf-uploader-2.0.9/gnos_upload_vcf.pl \\\n"
				+ " --gto-only --key /gnos.key --upload-url "+this.gnosMetadataUploadURL+" "
						+ " --metadata-urls "+this.normalMetdataURL+","+this.tumourMetdataURL+" \\\n"
						+ " --vcfs $SNV_FROM_INDEL_OXOG"+vcfs+" \\\n"
						+ " --tarballs $SNV_FROM_INDEL_TARBALL"+tars+" \\\n"
						+ " --tarball-md5sum-files $SNV_FROM_INDEL_TARBALL_MD5"+tarMD5Sums+" \\\n"
						+ " --vcf-idxs $SNV_FROM_INDEL_OXOG_INDEX"+vcfIndicies+" \\\n"
						+ " --vcf-md5sum-files $SNV_FROM_INDEL_OXOG_MD5"+vcfMD5Sums+" \\\n"
						+ " --vcf-idx-md5sum-files $SNV_FROM_INDEL_OXOG_INDEX_MD5"+vcfIndexMD5Sums+" \\\n"
						+ " --workflow-name OxoGWorkflow-OxoGFiltering \\\n"
						+ " --description-file /vcf/description.txt \\\n"
						+ " --workflow-version " + this.getVersion() + " \\\n"
						+ " --workflow-src-url https://github.com/ICGC-TCGA-PanCancer/OxoGWrapperWorkflow --workflow-url https://github.com/ICGC-TCGA-PanCancer/OxoGWrapperWorkflow  \"\n");
		
		generateAnalysisFilesVCFs.addParent(parentJob);
		
		//copy the analaysis.xml, manifest.xml *.gto files to /datastore/files_for_upload
		generateAnalysisFilesVCFs.getCommand().addArgument("cp /datastore/vcf-upload-prep/*/*/manifest.xml /datastore/files_for_upload/manifest.xml "
															+ " && cp /datastore/vcf-upload-prep/*/*/analysis.xml /datastore/files_for_upload/analysis.xml "
															+ " && cp /datastore/vcf-upload-prep/*/*/*.gto /datastore/files_for_upload/\n");
		//get the UUID that was submitted with the metadata
		//copyGTUploadFiles.getCommand().addArgument("VCF_UUID=$(grep server_path /datastore/files_for_upload/manifest.xml  | sed 's/.*server_path=\\\"\\(.*\\)\\\" .*/\1/g')\n");
		
		Job generateAnalysisFilesBAMs = this.getWorkflow().createBashJob("generate_analysis_files_for_BAM_upload");
		
		String bams = "";
		String bamIndicies = "";
		String bamMD5Sums = "";
		String bamIndexMD5Sums = "";
		generateAnalysisFilesBAMs.getCommand().addArgument("sudo chmod a+rw -R /datastore/variantbam_results/ &&\n");
		for (String file : this.filesForUpload.stream().filter(p -> p.contains(".bam") || p.contains(".bai") ).collect(Collectors.toList()) )
		{
			file = file.trim();
			//md5sum test_files/tumour_minibam.bam.bai | cut -d ' ' -f 1 > test_files/tumour_minibam.bai.md5
			generateAnalysisFilesBAMs.getCommand().addArgument(" md5sum "+file+" | cut -d ' ' -f 1 > "+file+".md5 ; \n");
			
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
							+ " Note the 'ANALYSIS_TYPE' is 'REFERENCE_ASSEMBLY' but a better term to describe this analysis is 'SEQUENCE_VARIATION' as defined by the EGA's SRA 1.5 schema."
							+ " Please note the reference used for alignment was hs37d, see ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/reference/phase2_reference_assembly_sequence/README_human_reference_20110707 for more information."
							+ " Briefly this is the integrated reference sequence from the GRCh37 primary assembly (chromosomal plus unlocalized and unplaced contigs),"
							+ " the rCRS mitochondrial sequence (AC:NC_012920), Human herpesvirus 4 type 1 (AC:NC_007605) and the concatenated decoy sequences (hs37d5cs.fa.gz)."
							+ " Variant calls may not be present for all contigs in this reference.";
		
		generateAnalysisFilesBAMs.getCommand().addArgument("\n docker run --rm --name=upload_bams -v /datastore/bam-upload-prep/:/vcf/ -v "+this.gnosKey+":/gnos.key -v /datastore/:/datastore/ "
				+ " pancancer/pancancer_upload_download:1.7 /bin/bash -c \"cat << DESCRIPTIONFILE > /vcf/description.txt\n"
				+ bamDescription
				+ "\nDESCRIPTIONFILE"
				+ " perl -I /opt/gt-download-upload-wrapper/gt-download-upload-wrapper-2.0.13/lib/ /opt/vcf-uploader/vcf-uploader-2.0.9/gnos_upload_vcf.pl \\\n"
				+ " --gto-only --key /gnos.key --upload-url "+this.gnosMetadataUploadURL+" "
						+ " --metadata-urls "+this.normalMetdataURL+","+this.tumourMetdataURL+" \\\n"
						+ " --bams "+bams+" \\\n"
						+ " --bam-bais "+bamIndicies+" \\\n"
						+ " --bam-md5sum-files "+bamMD5Sums+" \\\n"
						+ " --bam_bai-md5sum-files "+bamIndexMD5Sums+" \\\n"
						+ " --workflow-name OxoGWorkflow-variantbam \\\n"
						+ " --description-file /vcf/description.txt \\\n"
						+ " --workflow-version " + this.getVersion() + " \\\n"
						+ " --workflow-src-url https://github.com/ICGC-TCGA-PanCancer/OxoGWrapperWorkflow --workflow-url https://github.com/ICGC-TCGA-PanCancer/OxoGWrapperWorkflow  \"\n");
		generateAnalysisFilesBAMs.getCommand().addArgument("\n cp /datastore/bam-upload-prep/*/*/manifest.xml /datastore/variantbam_results/manifest.xml "
															+ " && cp /datastore/bam-upload-prep/*/*/analysis.xml /datastore/variantbam_results/analysis.xml "
															+ " && cp /datastore/bam-upload-prep/*/*/*.gto /datastore/variantbam_results/");
		generateAnalysisFilesBAMs.addParent(parentJob);

	
		String gnosServer = this.gnosMetadataUploadURL.replace("http://", "").replace("https://", "").replace("/", "");
		//Note: It was decided there should be two uploads: one for minibams and one for VCFs (for people who want VCFs but not minibams).
		Job uploadVCFResults = this.getWorkflow().createBashJob("upload VCF results");
		String uploadVCFCommand = "sudo chmod 0600 /datastore/credentials/rsync.key\n"
								+ "UPLOAD_PATH=$( echo \""+this.uploadURL+"\" | sed 's/\\(.*\\)\\:\\(.*\\)/\\2/g' )\n"
								+ "VCF_UUID=$(grep server_path /datastore/files_for_upload/manifest.xml  | sed 's/.*server_path=\\\"\\(.*\\)\\\" .*/\\1/g')\n"
								+ "( rsync -avz -e 'ssh -o UserKnownHostsFile=/datastore/credentials/known_hosts -o IdentitiesOnly=yes -o BatchMode=yes -o PasswordAuthentication=no -o PreferredAuthentications=publickey -i "+this.uploadKey+"'"
										+ " --rsync-path=\"mkdir -p $UPLOAD_PATH/"+gnosServer+"/$VCF_UUID && rsync\" /datastore/files_for_upload/ " + this.uploadURL+ "/"+gnosServer + "/$VCF_UUID ) ";
		uploadVCFCommand += (" || " + moveToFailed);
		uploadVCFResults.setCommand(uploadVCFCommand);
		uploadVCFResults.addParent(generateAnalysisFilesVCFs);

		
		Job uploadBAMResults = this.getWorkflow().createBashJob("upload BAM results");
		String uploadBAMcommand = "sudo chmod 0600 /datastore/credentials/rsync.key\n"
								+ "UPLOAD_PATH=$( echo \""+this.uploadURL+"\" | sed 's/\\(.*\\)\\:\\(.*\\)/\\2/g' )\n"
								+ "BAM_UUID=$(grep server_path /datastore/variantbam_results/manifest.xml  | sed 's/.*server_path=\\\"\\(.*\\)\\\" .*/\\1/g')\n"
								+ "( rsync -avz -e 'ssh -o UserKnownHostsFile=/datastore/credentials/known_hosts -o IdentitiesOnly=yes -o BatchMode=yes -o PasswordAuthentication=no -o PreferredAuthentications=publickey -i "+this.uploadKey+"'"
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
	private Job runAnnotator(String inputType, String workflowName, String vcfPath, String tumourBamPath, String normalBamPath, Job ...parents)
	{
		String outDir = "/datastore/files_for_upload/";
		String containerName = "pcawg-annotator_"+workflowName+"_"+inputType;
		String commandName ="run annotator for "+workflowName+" "+inputType; 
		if (vcfPath.contains("extracted_snvs"))
		{
			outDir+= "snvs_from_indels/";
			containerName += "_SNVs-from-INDELs";
			commandName += "_SNVs-from-INDELs";
		}
		String command = "";
		String annotatedFileName = this.aliquotID+"_annotated_"+workflowName+"_"+inputType+".vcf";
		
		Job annotatorJob = this.getWorkflow().createBashJob(commandName);
		if (!this.skipAnnotation)
		{
			// The "[ -f ${vcfPath} ]..." exists to handle cases where there is an SNV extracted from an INDEL.
			// We have to have a condition that checks at script-run time because we don't know if it will exist 
			// when the workflow engine first builds the scripts.
			// Also, the call to the ljdursi/pcawg-annotate container looks a little weird, (inside parens and redirected to a file),
			// but that seems to be the easiest way to get the capture the outpuyt from it. 
			command = "( ([ -f "+vcfPath+" ] \\\n"
							+ " && (docker run --rm --name="+containerName+" -v "+vcfPath+":/input.vcf "
							+ " -v "+tumourBamPath+":/tumour_minibam.bam "
							+ " -v "+normalBamPath+":/normal_minibam.bam "
							+ " ljdursi/pcawg-annotate  "
							+ " "+inputType+" /input.vcf /normal_minibam.bam /tumour_minibam.bam ) > "+outDir+"/"+annotatedFileName+" ) \\\n"
							+ " && ( bgzip -f -c "+outDir+annotatedFileName+" > "+outDir+"/"+annotatedFileName+".gz && "
							+ " tabix -p vcf "+outDir+"/"+annotatedFileName+".gz ; "
							+ " ) ) \\\n" ;
			
			
			String moveToFailed = GitUtils.gitMoveCommand("running-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, this.getWorkflowBaseDir() + "/scripts/");
			command += " || " + moveToFailed;
		}
		filesForUpload.add("/datastore/files_for_upload/"+annotatedFileName+".gz ");
		filesForUpload.add("/datastore/files_for_upload/"+annotatedFileName+".gz.tbi ");
		
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
		System.out.println(filesForUpload);
		String broadOxogSNVFileName = filesForUpload.stream().filter(p -> p.contains("broad-mutect") && p.endsWith("somatic.snv_mnv.pass-filtered.oxoG.vcf.gz")).collect(Collectors.toList()).get(0);
		String sangerOxogSNVFileName = filesForUpload.stream().filter(p -> p.contains("svcp_") && p.endsWith("somatic.snv_mnv.pass-filtered.oxoG.vcf.gz")).collect(Collectors.toList()).get(0);
		String museOxogSNVFileName = filesForUpload.stream().filter(p -> p.contains("MUSE") && p.endsWith("somatic.snv_mnv.oxoG.vcf.gz")).collect(Collectors.toList()).get(0);
		String dkfzEmbleOxogSNVFileName = filesForUpload.stream().filter(p -> p.contains("dkfz-snvCalling") && p.endsWith("somatic.snv_mnv.pass-filtered.oxoG.vcf.gz")).collect(Collectors.toList()).get(0);
		
		String broadOxoGSNVFromIndelFileName = broadOxogSNVFileName.replace("/oxog_results/", "/oxog_results_extracted_snvs/");
		String sangerOxoGSNVFromIndelFileName = sangerOxogSNVFileName.replace("/oxog_results/", "/oxog_results_extracted_snvs/");
		String dkfzEmblOxoGSNVFromIndelFileName = dkfzEmbleOxogSNVFileName.replace("/oxog_results/", "/oxog_results_extracted_snvs/");
		

		Job broadIndelAnnotatorJob = this.runAnnotator("indel", "broad", this.broadNormalizedIndelVCFName, this.tumourMinibamPath,this.normalMinibamPath, parents);
		Job dfkzEmblIndelAnnotatorJob = this.runAnnotator("indel", "dkfz_embl", this.dkfzEmblNormalizedIndelVCFName, this.tumourMinibamPath, this.normalMinibamPath, broadIndelAnnotatorJob);
		Job sangerIndelAnnotatorJob = this.runAnnotator("indel","sanger", this.sangerNormalizedIndelVCFName, this.tumourMinibamPath, this.normalMinibamPath, dfkzEmblIndelAnnotatorJob);

		Job broadSNVAnnotatorJob = this.runAnnotator("SNV", "broad",broadOxogSNVFileName, this.tumourMinibamPath, this.normalMinibamPath, parents);
		Job dfkzEmblSNVAnnotatorJob = this.runAnnotator("SNV", "dkfz_embl",dkfzEmbleOxogSNVFileName, this.tumourMinibamPath, this.normalMinibamPath, broadSNVAnnotatorJob);
		Job sangerSNVAnnotatorJob = this.runAnnotator("SNV","sanger",sangerOxogSNVFileName, this.tumourMinibamPath, this.normalMinibamPath, dfkzEmblSNVAnnotatorJob);
		Job museSNVAnnotatorJob = this.runAnnotator("SNV","muse",museOxogSNVFileName, this.tumourMinibamPath, this.normalMinibamPath, dfkzEmblSNVAnnotatorJob);

		Job broadSNVFromIndelAnnotatorJob = this.runAnnotator("SNV","broad", broadOxoGSNVFromIndelFileName, this.tumourMinibamPath, this.normalMinibamPath, parents);
		Job dfkzEmblSNVFromIndelAnnotatorJob = this.runAnnotator("SNV","dkfz_embl", dkfzEmblOxoGSNVFromIndelFileName, this.tumourMinibamPath, this.normalMinibamPath, broadSNVFromIndelAnnotatorJob);
		Job sangerSNVFromIndelAnnotatorJob = this.runAnnotator("SNV","sanger", sangerOxoGSNVFromIndelFileName, this.tumourMinibamPath, this.normalMinibamPath, dfkzEmblSNVFromIndelAnnotatorJob);
		
		finalAnnotatorJobs.add(sangerSNVFromIndelAnnotatorJob);
		finalAnnotatorJobs.add(sangerSNVAnnotatorJob);
		finalAnnotatorJobs.add(sangerIndelAnnotatorJob);
		finalAnnotatorJobs.add(museSNVAnnotatorJob);
		
		return finalAnnotatorJobs;
	}
	
	/**
	 * Build the workflow!!
	 */
	@Override
	public void buildWorkflow() {
		try {
			this.init();
			this.generateRulesFile();
			// Pull the repo.
			Job configJob = GitUtils.gitConfig(this.getWorkflow(), this.GITname, this.GITemail);
			
			Job copy = this.copyCredentials(configJob);
			
			Job pullRepo = GitUtils.pullRepo(this.getWorkflow(), this.GITPemFile, this.JSONrepo, this.JSONrepoName, this.JSONlocation);
			pullRepo.addParent(copy);
			
			Job installTabix = this.installTabix(pullRepo);
			
			// indicate job is in downloading stage.
			String pathToScripts = this.getWorkflowBaseDir() + "/scripts";
			Job move2download = GitUtils.gitMove("queued-jobs", "downloading-jobs", this.getWorkflow(), this.JSONlocation, this.JSONrepoName, this.JSONfolderName, this.GITname, this.GITemail, this.gitMoveTestMode, this.JSONfileName, pathToScripts ,installTabix);
			Job move2running;
			if (!skipDownload) {
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
				Job downloadTumourBam = this.getBAM(downloadNormalBam,BAMType.tumour, this.bamTumourIndexObjectID,this.bamTumourObjectID);
				
				// After we've downloaded all VCFs on a per-workflow basis, we also need to do a vcfcombine 
				// on the *types* of VCFs, for the minibam generator. The per-workflow combined VCFs will
				// be used by the OxoG filter. These three can be done in parallel because they all require the same inputs, 
				// but none require the inputs of the other and they are not very intense jobs.
				// indicate job is running.
				move2running = GitUtils.gitMove( "downloading-jobs", "running-jobs", this.getWorkflow(),
						this.JSONlocation, this.JSONrepoName, this.JSONfolderName, this.GITname, this.GITemail, this.gitMoveTestMode, this.JSONfileName, pathToScripts
						, downloadSangerVCFs, downloadDkfzEmblVCFs, downloadBroadVCFs, downloadMuseVCFs, downloadNormalBam, downloadTumourBam);
			}
			else {
				// If user is skipping download, then we will just move directly to runnning...
				move2running = GitUtils.gitMove("downloading-jobs", "running-jobs", this.getWorkflow(), this.JSONlocation, this.JSONrepoName, this.JSONfolderName, this.GITname, this.GITemail, this.gitMoveTestMode, this.JSONfileName , pathToScripts,move2download);
			}

			Job sangerPassFilter = this.passFilterWorkflow(Pipeline.sanger, move2running);
			Job broadPassFilter = this.passFilterWorkflow(Pipeline.broad, move2running);
			Job dkfzemblPassFilter = this.passFilterWorkflow(Pipeline.dkfz_embl, move2running);
			// No, we're not going to filter the Muse SNV file.
			
			//Function<String,String> f = (x) -> { return x.replace(".vcf.gz",".pass-filtered.vcf.gz"); };
			//this.sangerSNVName = (String) f.apply(this.sangerSNVName);
			//this.sangerIndelName = (String) f.apply(this.sangerIndelName);
			
			//update all filenames to include ".pass-filtered."
			this.sangerSNVName = this.sangerSNVName.replace(".vcf.gz", ".pass-filtered.vcf.gz");
			this.sangerIndelName = this.sangerIndelName.replace(".vcf.gz", ".pass-filtered.vcf.gz");
			this.sangerSVName = this.sangerSVName.replace(".vcf.gz", ".pass-filtered.vcf.gz");
			
			this.broadSNVName = this.broadSNVName.replace(".vcf.gz", ".pass-filtered.vcf.gz");
			this.broadIndelName = this.broadIndelName.replace(".vcf.gz", ".pass-filtered.vcf.gz");
			this.broadSVName = this.broadSVName.replace(".vcf.gz", ".pass-filtered.vcf.gz");
			
			this.dkfzEmblSNVName = this.dkfzEmblSNVName.replace(".vcf.gz", ".pass-filtered.vcf.gz");
			this.dkfzEmblIndelName = this.dkfzEmblIndelName.replace(".vcf.gz", ".pass-filtered.vcf.gz");
			this.dkfzEmblSVName = this.dkfzEmblSVName.replace(".vcf.gz", ".pass-filtered.vcf.gz");
			
			// OxoG will run after move2running. Move2running will run after all the jobs that perform input file downloads and file preprocessing have finished.  
			Job sangerPreprocessVCF = this.preProcessIndelVCF(sangerPassFilter, Pipeline.sanger,"/"+ this.sangerGnosID +"/"+ this.sangerIndelName);
			Job dkfzEmblPreprocessVCF = this.preProcessIndelVCF(dkfzemblPassFilter, Pipeline.dkfz_embl, "/"+ this.dkfzemblGnosID +"/"+ this.dkfzEmblIndelName);
			Job broadPreprocessVCF = this.preProcessIndelVCF(broadPassFilter, Pipeline.broad, "/"+ this.broadGnosID +"/"+ this.broadIndelName);
			
			Job combineVCFsByType = this.combineVCFsByType( sangerPreprocessVCF, dkfzEmblPreprocessVCF, broadPreprocessVCF);
			
			Job oxoG = this.doOxoG(combineVCFsByType);
			Job oxoGSnvsFromIndels = this.doOxoGSnvsFromIndels(oxoG);
			// variantbam jobs will run parallel to each other. variant seems to only use a *single* core, but runs long ( 60 - 120 min on OpenStack);
			Job normalVariantBam = this.doVariantBam(combineVCFsByType,BAMType.normal,"/datastore/bam/normal/"+this.normalBamGnosID+"/"+this.normalBAMFileName);
			Job tumourVariantBam = this.doVariantBam(combineVCFsByType,BAMType.tumour,"/datastore/bam/tumour/"+this.tumourBamGnosID+"/"+this.tumourBAMFileName);

			List<Job> annotationJobs = this.doAnnotations(oxoGSnvsFromIndels, tumourVariantBam, normalVariantBam, oxoG);
			
			//Now do the Upload
			if (!skipUpload)
			{
				// indicate job is in uploading stage.
				//Job move2uploading = GitUtils.gitMove("running-jobs", "uploading-jobs", this.getWorkflow(), this.JSONlocation, this.JSONrepoName, this.JSONfolderName, this.GITname, this.GITemail, this.gitMoveTestMode, this.JSONfileName, pathToScripts , indelAnnotatorJob, snvAnnotatorJob, snvFromIndelAnnotatorJob, oxoG, oxoGSnvsFromIndels, normalVariantBam, tumourVariantBam);
				Job move2uploading = this.gitMove( "running-jobs", "uploading-jobs", annotationJobs.toArray(new Job[annotationJobs.size()]));
				Job uploadResults = doUpload(move2uploading);
				// indicate job is complete.
				this.gitMove( "uploading-jobs", "completed-jobs", uploadResults);
			}
			else
			{
				//GitUtils.gitMove( "running-jobs", "completed-jobs", this.getWorkflow(), this.JSONlocation, this.JSONrepoName, this.JSONfolderName, this.GITname, this.GITemail, this.gitMoveTestMode, this.JSONfileName ,pathToScripts,  indelAnnotatorJob, snvAnnotatorJob, snvFromIndelAnnotatorJob, oxoG, oxoGSnvsFromIndels, normalVariantBam, tumourVariantBam);
				this.gitMove( "running-jobs", "completed-jobs",annotationJobs.toArray(new Job[annotationJobs.size()]));
			}
	
		}
		catch (Exception e)
		{
			throw new RuntimeException ("Exception caught: "+e.getMessage(), e);
		}
	}
}
