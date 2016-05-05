package com.github.seqware.jobgenerators;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import com.github.seqware.GitUtils;
import com.github.seqware.OxoGWrapperWorkflow.BAMType;
import com.github.seqware.OxoGWrapperWorkflow.Pipeline;
import com.github.seqware.OxoGWrapperWorkflow.VCFType;
import com.github.seqware.TemplateUtils;
import com.github.seqware.VcfInfo;

import net.sourceforge.seqware.pipeline.workflowV2.AbstractWorkflowDataModel;
import net.sourceforge.seqware.pipeline.workflowV2.model.Job;

public class PreprocessJobGenerator extends JobGeneratorBase {

	private String tumourAliquotID;

	public PreprocessJobGenerator(String JSONlocation, String JSONrepoName, String JSONfolderName, String JSONfileName) {
		super(JSONlocation, JSONrepoName, JSONfolderName, JSONfileName);
	}
	
	/**
	 * Perform filtering on all VCF files for a given workflow.
	 * Filtering involves removing lines that are not "PASS" or "."
	 * Output files will have ".pass-filtered." in their name.
	 * @param workflowName The workflow to PASS filter
	 * @param parents List of parent jobs.
	 * @return
	 */
	public Job passFilterWorkflow(AbstractWorkflowDataModel workflow, Pipeline workflowName, Job ... parents)
	{
		Job passFilter = workflow.getWorkflow().createBashJob("pass filter "+workflowName);
		String moveToFailed = GitUtils.gitMoveCommand("running-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, workflow.getWorkflowBaseDir() + "/scripts/");
		//If we were processing MUSE files we would also need to filter for tier* but we're NOT processing MUSE files so 
		//we don't need to worry about that for now.
		Map<String,Object> context = new HashMap<String,Object>(2);
		context.put("workflowName", workflowName.toString());
		context.put("moveToFailed", moveToFailed);
		String renderedTemplate;
		
		renderedTemplate = TemplateUtils.getRenderedTemplate(context, "passFilter.template");
		
		passFilter.setCommand(renderedTemplate);
		
		Arrays.stream(parents).forEach(parent -> passFilter.addParent(parent));
		
		return passFilter;
	}
	
	/**
	 * Pre-processes INDEL VCFs. Normalizes INDELs and extracts SNVs from normalized INDELs.
	 * @param parent
	 * @param workflowName The name of the workflow whose files will be pre-processed.
	 * @param vcfName The name of the INDEL VCF to normalize.
	 * @return
	 */
	public Job preProcessIndelVCF(AbstractWorkflowDataModel workflow, Job parent, Pipeline workflowName, String vcfName, String refFile, Consumer<String> updateFilesForUpload, Consumer<VcfInfo> updateExtractedSNVs, Consumer<VcfInfo> updateNormalizedINDELs)
	{
		//System.out.println("DEBUG: preProcessIndelVCF: "+workflowName+" ; "+vcfName + " ; "+tumourAliquotID);
		String outDir = "/datastore/vcf/"+workflowName;
		String normalizedINDELName = this.tumourAliquotID+ "_"+ workflowName+"_somatic.indel.pass-filtered.bcftools-norm.vcf.gz";
		String extractedSNVVCFName = this.tumourAliquotID+ "_"+ workflowName+"_somatic.indel.pass-filtered.bcftools-norm.extracted-snvs.vcf";
		String fixedIndel = vcfName.replace("indel.", "indel.fixed.").replace(".gz", ""); //...because the fixed indel will not be a gz file - at least not immediately.
		Job bcfToolsNormJob = workflow.getWorkflow().createBashJob("normalize "+workflowName+" Indels");
		String sedTab = "\\\"$(echo -e '\\t')\\\"";
		String runBCFToolsNormCommand = TemplateUtils.getRenderedTemplate(Arrays.stream(new String[][]{
			{ "sedTab", sedTab }, { "outDir", outDir }, { "vcfName", vcfName }, { "workflowName", workflowName.toString() } ,
			{ "refFile", refFile }, { "fixedIndel", fixedIndel }, { "normalizedINDELName", normalizedINDELName },
			{ "tumourAliquotID", this.tumourAliquotID }
		}).collect(this.collectToMap), "bcfTools.template");

		String moveToFailed = GitUtils.gitMoveCommand("running-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, workflow.getWorkflowBaseDir() + "/scripts/");				 
		runBCFToolsNormCommand += (" || " + moveToFailed );
		
		bcfToolsNormJob.setCommand(runBCFToolsNormCommand);
		bcfToolsNormJob.addParent(parent);
		
		//Normalized INDELs should be indexed uploaded
		
		updateFilesForUpload.accept(outDir+"/"+normalizedINDELName);
		updateFilesForUpload.accept(outDir+"/"+normalizedINDELName+".tbi");
		
		Job extractSNVFromIndel = workflow.getWorkflow().createBashJob("extracting SNVs from "+workflowName+" INDEL");
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
		extractedSnv.setOriginatingTumourAliquotID(this.tumourAliquotID);
		updateExtractedSNVs.accept(extractedSnv);
		
		VcfInfo normalizedIndel = new VcfInfo();
		normalizedIndel.setFileName(outDir + "/"+normalizedINDELName);
		normalizedIndel.setOriginatingPipeline(workflowName);
		normalizedIndel.setVcfType(VCFType.indel);
		normalizedIndel.setOriginatingTumourAliquotID(this.tumourAliquotID);
		updateNormalizedINDELs.accept(normalizedIndel);

		return extractSNVFromIndel;
	}

	/**
	 * This will combine VCFs from different workflows by the same type. All INDELs will be combined into a new output file,
	 * all SVs will be combined into a new file, all SNVs will be combined into a new file. 
	 * @param parents
	 * @return
	 */
	public Job combineVCFsByType(AbstractWorkflowDataModel workflow, List<VcfInfo> nonIndels, List<VcfInfo> indels, Consumer<VcfInfo> updateMergedVCFs, BAMType bamType, Job ... parents)
	{
		HashMap<String,String> argsMap = new HashMap<>();
		
		//Create symlinks to the files in the proper directory.
		Job prepVCFs = workflow.getWorkflow().createBashJob("create links to VCFs");
		StringBuffer prepCommandSb = new StringBuffer();
		StringBuffer combineVcfArgsSb = new StringBuffer();

		Consumer<VcfInfo> setCommandStrings = vcfInfo -> {
			String linkTarget = " /datastore/vcf/"+vcfInfo.getOriginatingTumourAliquotID()+"_"+vcfInfo.getOriginatingPipeline().toString()+"_"+vcfInfo.getVcfType().toString()+".vcf ";
			String linkSource;
			if (vcfInfo.getVcfType() != VCFType.indel)
			{
				linkSource = "/datastore/vcf/"+vcfInfo.getOriginatingPipeline().toString()+"/"+vcfInfo.getPipelineGnosID()+"/"+vcfInfo.getFileName();
			}
			else
			{
				//the normalized indels already have a full file path in fileName.
				linkSource = vcfInfo.getFileName();
			}
			prepCommandSb.append( " ( [ ! -L "+linkTarget+" ] && ln -s " + linkSource+" "+ linkTarget+" ) && \\\n" );

			//The arguments for vcf_merge_by_type.pl are keyed by "vcfType:pipeline". This will allow merging across tumours, for multi-tumour scenarios.
			String key = vcfInfo.getVcfType().toString()+":"+vcfInfo.getOriginatingPipeline().toString();
			String prevValue = argsMap.get(key);
			String value = (prevValue!=null && prevValue.trim().length()>0 ? prevValue+"," : "")
							+ vcfInfo.getOriginatingTumourAliquotID() + "_" + vcfInfo.getOriginatingPipeline().toString() + "_"+vcfInfo.getVcfType().toString()+".vcf";
			argsMap.put(key, value);
		};

		indels.forEach(setCommandStrings);
		nonIndels.forEach(setCommandStrings);
		
		for (String k : argsMap.keySet())
		{
			String value = argsMap.get(k);
			String parts[] = k.split(":");
			String vcfType = parts[0];
			String pipeline = parts[1];
			//value will be a comma-separated list of filenames.
			combineVcfArgsSb.append( " --" + pipeline + "_" + vcfType+" "+value+" \\\n" );
		}
		
		String prepCommand = "";
		String combineVcfArgs = "";
		prepCommand +="\n ( ( [ -d /datastore/merged_vcfs ] || sudo mkdir -p /datastore/merged_vcfs/ ) && sudo chmod a+rw /datastore/merged_vcfs && \\\n";
		prepCommand += prepCommandSb.toString();
		combineVcfArgs = combineVcfArgsSb.toString();
		prepCommand = prepCommand.substring(0,prepCommand.lastIndexOf("&&"));
		String moveToFailed = GitUtils.gitMoveCommand("running-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, workflow.getWorkflowBaseDir() + "/scripts/");
		prepCommand += (") || " + moveToFailed);
		
		prepVCFs.setCommand(prepCommand);

		Arrays.stream(parents).forEach(parent -> prepVCFs.addParent(parent));

		Job vcfCombineJob = workflow.getWorkflow().createBashJob("combining VCFs by type");

		//run the merge script, then bgzip and index them all.
		String combineCommand = "( perl "+workflow.getWorkflowBaseDir()+"/scripts/vcf_merge_by_type.pl \\\n"
				+ combineVcfArgs + "\\\n"
				+ " --indir /datastore/vcf/ --outdir /datastore/merged_vcfs/ \\\n ) || "+moveToFailed;

		vcfCombineJob.setCommand(combineCommand);
		vcfCombineJob.addParent(prepVCFs);
		
		//these files names are hard-coded in runVariantbam.template. So, either get rid of them here (if they're not used anywhere else),
		//or add them as parameters to the template.
		VcfInfo mergedSnvVcf = new VcfInfo();
		mergedSnvVcf.setFileName("snv.clean.sorted.vcf");
		mergedSnvVcf.setVcfType(VCFType.snv);
		VcfInfo mergedSvVcf = new VcfInfo();
		mergedSvVcf.setFileName("sv.clean.sorted.vcf");
		mergedSvVcf.setVcfType(VCFType.sv);
		VcfInfo mergedIndelVcf = new VcfInfo();
		mergedIndelVcf.setFileName("indel.clean.sorted.vcf");
		mergedIndelVcf.setVcfType(VCFType.indel);
		updateMergedVCFs.accept(mergedSnvVcf);
		updateMergedVCFs.accept(mergedSvVcf);
		updateMergedVCFs.accept(mergedIndelVcf);

		return vcfCombineJob;
	}

	public String getTumourAliquotID() {
		return this.tumourAliquotID;
	}

	public void setTumourAliquotID(String tumourAliquotID) {
		this.tumourAliquotID = tumourAliquotID;
	}
	
}
