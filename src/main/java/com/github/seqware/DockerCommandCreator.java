package com.github.seqware;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.seqware.OxoGWrapperWorkflow.BAMType;

public class DockerCommandCreator {

	static String createDockerRunCommand(String imageName, Map<String, String> mountedObjects, List<String> runOpts,
			String containerCommand, List<String> containerCommandArgs) {
		StringBuilder sb = new StringBuilder();

		sb.append(" docker run ");

		for (String opt : runOpts) {
			sb.append(" ").append(opt).append(" ");
		}
		for (String v : mountedObjects.keySet()) {
			sb.append(" -v ").append(v).append(":").append(mountedObjects.get(v)).append(" ");
		}
		sb.append(imageName).append(" ");

		sb.append(" /bin/bash -c \"");
		sb.append(containerCommand);
		for (String arg : containerCommandArgs) {
			sb.append(" ").append(arg).append(" ");
		}
		sb.append("\"");

		return sb.toString();
	}

	static String createVariantBamCommand(BAMType bamType, String outputFileName, String bamPath, String snvVCF, String svVCF, String indelVCF, int svPadding, int snvPadding, int indelPadding, String tumourID) {
		String command = "";

		Map<String, String> mountedObjects = new HashMap<String, String>(6);
		mountedObjects.put(snvVCF, "/snv.vcf");
		mountedObjects.put(svVCF, "/sv.vcf");
		mountedObjects.put(indelVCF, "/indel.vcf");
		mountedObjects.put("/datastore/padding_rules.txt", "/rules.txt");
		mountedObjects.put("/datastore/variantbam_results/", "/outdir/");
		mountedObjects.put(bamPath, "/input.bam");
		List<String> runOpts = new ArrayList<String>(2);
		runOpts.add("--rm");
		runOpts.add("--name=\"oxog_variantbam_" + bamType + (bamType == BAMType.tumour ? "_with_tumour_"+tumourID:"") + "\"");
		List<String> containerCommandArgs = new ArrayList<String>(12);
		containerCommandArgs.add("-o /outdir/"+outputFileName);
		containerCommandArgs.add("-i /input.bam");
		containerCommandArgs.add("-l /snv.vcf");
		containerCommandArgs.add("-l /sv.vcf");
		containerCommandArgs.add("-l /indel.vcf");
		//Rules file seems to be having problems (variant doesn't recognize the \n between each line. weird).
		//So just do the rules in-line and separate them with a "%".
		containerCommandArgs.add("-r 'pad["+svPadding+"];mlregion@/sv.vcf%pad["+snvPadding+"];mlregion@/snv.vcf%pad["+indelPadding+"]mlregion@/indel.vcf' ");
		//UGLY!! - rethink this whole DockerComandCreator...
		containerCommandArgs.add(" && samtools index  /outdir/"+outputFileName);

		command = DockerCommandCreator.createDockerRunCommand("oxog:160329", mountedObjects, runOpts,
				"  /cga/fh/pcawg_pipeline/modules/VariantBam/variant", containerCommandArgs);

		return command;
	}
	
	static String createGetVCFCommand(String workflowName, String storageSource, String outDir, String downloadObjects)
	{
		String command = "";
		Map<String,String> mountedObjects = new HashMap<String,String>(3);
		mountedObjects.put(outDir+"/logs/", "/icgc/icgc-storage-client/logs/");
		mountedObjects.put("/datastore/credentials/collab.token", "/icgc/icgc-storage-client/conf/application.properties");
		mountedObjects.put(outDir+"/", "/downloads/");
		
		List<String> runOpts = new ArrayList<String>(2);
		runOpts.add("--rm");
		runOpts.add("--name get_vcf_"+workflowName);
		runOpts.add("-e STORAGE_PROFILE="+storageSource);
		
		List<String> containerArgs = new ArrayList<String>();
		containerArgs.add(downloadObjects);

		command = DockerCommandCreator.createDockerRunCommand("icgc-storage-client", mountedObjects, runOpts, "",containerArgs);
		return command;
	}
	
	static String createOxoGCommand(String aliquotID, String normalBam, String tumourBam, String oxoqScore, String ... VCFs)
	{
		String command = "";
		
		Map<String, String> mountedObjects = new HashMap<String, String>();
		List<String> runOpts = new ArrayList<String>();
		String containerCommand = "";
		List<String> containerCommandArgs = new ArrayList<String>();
		command = DockerCommandCreator.createDockerRunCommand("oxog", mountedObjects, runOpts, containerCommand, containerCommandArgs);
		
		return command;
	}
}
