package com.github.seqware;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import com.github.seqware.OxoGWrapperWorkflow.VCFType;

public class INIGenerator {

	private static String ini = "JSONrepo = https://github.com/ICGC-TCGA-PanCancer/oxog-ops.git\n" + "JSONrepoName = oxog-ops\n"
			+ "JSONfolderName = oxog-collab-jobs-test\n" + "JSONlocation = /home/seqware/gitroot/\n" +
			// "JSONfileName = SomeDonor_1234.json\n"+
			"GITemail = denis.yuen+icgc@gmail.com\n" + "GITname = icgc-bot\n"
			+ "GITPemFile = /home/seqware/.gnos/git.pem\n"
			+ "uploadURL = oicr@192.170.233.206:~/incoming/bulk_upload/\n";

	private static Map<String, Object> getDataFromJSON(String pathToJSON) {
		Map<String, Object> inputsFromJSON = JSONUtils.processJSONFile(pathToJSON);
		return inputsFromJSON;
	}

	private static String mapToINI(Map<String, Object> m, String prefix) {
		StringBuilder sb = new StringBuilder();

		for (String k : m.keySet()) {
			if (m.get(k) instanceof String) {
				// System.out.println(prefix + " " + m.get(k));
				// Some things *don't* need to be printed
				if (!(k.equals(JSONUtils.TAG))) {
					sb.append(prefix.equals("") ? "" : prefix + "_").append(k).append(" = ").append(m.get(k))
							.append("\n");
				}
			} else if (m.get(k) instanceof Map) {
				// System.out.println(prefix + " " + m.get(k));
				String newPrefix = prefix.equals("") ? "" : prefix;

				if (((Map<String, Object>) m.get(k)).containsKey(JSONUtils.TAG)) {
					newPrefix = (String) ((Map<String, Object>) m.get(k)).get(JSONUtils.TAG);
				} else if (k.equals(JSONUtils.DATA)) {
					newPrefix += "_" + k;
				} else if (k.equals(JSONUtils.INDEX)) {
					newPrefix += "_" + k;
				} else if (k.equals(VCFType.sv.toString())) {
					newPrefix += "_" + k;
				} else if (k.equals(VCFType.indel.toString())) {
					newPrefix += "_" + k;
				} else if (k.equals(VCFType.snv.toString())) {
					newPrefix += "_" + k;
				}
				sb.append(mapToINI((Map<String, Object>) m.get(k), newPrefix));
			}
		}
		return sb.toString();
	}

	public static void main(String[] args) throws Exception {
		// Path to the JSON file will be args[0]
		if (args.length > 0) {
			Map<String, Object> fromJSON = getDataFromJSON(args[0]);

			StringBuilder sb = new StringBuilder();

			String iniFromJSON = mapToINI(fromJSON, "");
			sb.append(ini);
			sb.append(iniFromJSON);
			String donorID = (String) fromJSON.get(JSONUtils.SUBMITTER_DONOR_ID);
			String projectCode = (String) fromJSON.get(JSONUtils.PROJECT_CODE);
			sb.append("JSONfileName = " + projectCode + "." + donorID + ".json");

			Files.write(Paths.get("./" + donorID + ".INI"), sb.toString().getBytes());
			System.out.println("Writing file: " + "./" + donorID + ".INI");
		} else {
			throw new Exception("You must pass the path to a JSON file as the only argument to this program.");
		}
	}

}
