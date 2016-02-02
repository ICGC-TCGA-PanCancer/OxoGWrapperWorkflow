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

	static String createVariantBamCommand(BAMType bamType, String bamPath, String snvVCF, String svVCF, String indelVCF) {
		String command = "";

		Map<String, String> mountedObjects = new HashMap<String, String>(6);
		mountedObjects.put(snvVCF, "/snv.vcf");
		mountedObjects.put(svVCF, "/sv.vcf");
		mountedObjects.put(indelVCF, "/indel.vcf");
		mountedObjects.put("/datastore/padding_rules.text", "/rules.txt");
		mountedObjects.put("/datastore/variantbam_results/", "/outdir/");
		mountedObjects.put(bamPath, "/input.bam");
		List<String> runOpts = new ArrayList<String>(2);
		runOpts.add("--rm");
		runOpts.add("--name=\"oxog_variantbam_" + bamType + "\"");
		List<String> containerCommandArgs = new ArrayList<String>(12);
		containerCommandArgs.add("-o /outdir/minibam_" + bamType + ".bam");
		containerCommandArgs.add("-i /input.bam");
		containerCommandArgs.add("-l /snv.vcf");
		containerCommandArgs.add("-l /sv.vcf");
		containerCommandArgs.add("-l /indel.vcf");

		DockerCommandCreator.createDockerRunCommand("oxog", mountedObjects, runOpts,
				"/cga/fh/pcawg_pipeline/modules/VariantBam/variant", containerCommandArgs);

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
