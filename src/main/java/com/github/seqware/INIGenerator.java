package com.github.seqware;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

public class INIGenerator {
	
	private static String ini = "uploadURL = http://some.rsync.server.com/oxogUpload/\n"+
					"JSONrepo = https://github.com/ICGC-TCGA-PanCancer/oxog-ops.git\n"+ 
					"JSONrepoName = oxog-ops\n"+
					"JSONfolderName = oxog-collab-jobs-test\n"+
					"JSONlocation = /home/seqware/gitroot/\n"+
					//"JSONfileName = SomeDonor_1234.json\n"+ 
					"GITemail = denis.yuen+icgc@gmail.com\n"+
					"GITname = icgc-bot\n"+
					"GITPemFile = /home/seqware/.gnos/git.pem\n";
	
	private static Map<String, Object> getDataFromJSON(String pathToJSON) {
		Map<String, Object> inputsFromJSON = JSONUtils.processJSONFile(pathToJSON);
		return inputsFromJSON;
	}
	
	public static void main(String[] args) throws Exception {
		// Path to the JSON file will be args[0]
		if (args.length > 0)
		{
			Map<String, Object> fromJSON = getDataFromJSON(args[0]);
			
			StringBuilder sb = new StringBuilder();
			for (String k : fromJSON.keySet())
			{
				sb.append(k).append(" = ").append(fromJSON.get(k)).append("\n");
			}
			sb.append(ini);
			String donorID = (String) fromJSON.get(JSONUtils.SUBMITTER_DONOR_ID);
			String projectCode = (String) fromJSON.get(JSONUtils.PROJECT_CODE);
			sb.append("JSONfileName = "+projectCode+"."+donorID+".json");
			
			
			Files.write(Paths.get("./"+donorID+".INI"), sb.toString().getBytes());
			System.out.println("Writing file: "+"./"+donorID+".INI");
		}
		else
		{
			throw new Exception("You must pass the path to a JSON file as the only argument to this program.");
		}
	}

}
