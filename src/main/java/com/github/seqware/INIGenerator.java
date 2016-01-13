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
					"GITemail = icgc-bot@users.noreply.github.com\n"+
					"GITname = \"ICGC Automation\"\n"+
					"GITPemFile = /home/ubuntu/.gnos/git.pem\n";
	
	private static Map<String,String> getDataFromJSON(String pathToJSON) {
		Map<String,String> inputsFromJSON = JSONUtils.processJSONFile(pathToJSON);
		return inputsFromJSON;
	}
	
	public static void main(String[] args) throws Exception {
		// Path to the JSON file will be args[0]
		if (args.length > 0)
		{
			Map<String,String> fromJSON = getDataFromJSON(args[0]);
			
			StringBuilder sb = new StringBuilder();
			for (String k : fromJSON.keySet())
			{
				sb.append(k).append(" = ").append(fromJSON.get(k)).append("\n");
			}
			sb.append(ini);
			String donorID = fromJSON.get(JSONUtils.SUBMITTER_DONOR_ID);
			sb.append("JSONfileName = "+donorID+".json");
			
			
			Files.write(Paths.get("./"+donorID+".INI"), sb.toString().getBytes());
		}
		else
		{
			throw new Exception("You must pass the path to a JSON file as the only argument to this program.");
		}
	}

}
