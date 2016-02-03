package com.github.seqware;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;

public abstract class JSONUtils {

	static final String OXOQ_SCORE = "OxoQScore";
	static final String BROAD_VCF_OBJECT_ID = "broadVCFObjectID";
	static final String DKFZEMBL_VCF_OBJECT_ID = "dkfzemblVCFObjectID";
	static final String SANGER_VCF_OBJECT_ID = "sangerVCFObjectID";
	static final String BAM_NORMAL_OBJECT_ID = "bamNormalObjectID";
	static final String BAM_NORMAL_INDEX_OBJECT_ID = "bamNormalBaiObjectID";
	static final String BAM_TUMOUR_INDEX_OBJECT_ID = "bamTumourBaiObjectID";
	static final String BAM_NORMAL_METADATA_URL = "bamNormalMetadataURL";
	static final String BAM_TUMOUR_OBJECT_ID = "bamTumourObjectID";
	static final String BAM_TUMOUR_METADATA_URL = "bamTumourMetadataURL";
	static final String MUSE_VCF_OBJECT_ID = "museVCFObjectID";
	static final String ALIQUOT_ID = "aliquotID";
	static final String SUBMITTER_DONOR_ID = "submitterDonorID";
	static final String PROJECT_CODE = "projectCode";

	static final String FILE = "file";
	static final String INDEX = "index";
	
	static final String NORMAL_BAM_INFO = "normalBamInfo";
	static final String TUMOUR_BAM_INFO = "tumourBamInfo";
	static final String SANGER_VCF_INFO = "sangerVcfInfo";
	static final String DKFZEMBL_VCF_INFO = "dkfzemblVcfInfo";
	static final String BROAD_VCF_INFO = "broadVcfInfo";
	static final String MUSE_VCF_INFO = "museVcfInfo";
	
	public static Map<String, Object> processJSONFile(String filePath) {

		Map<String, Object> results = new HashMap<String, Object>(8);

		Type simpleMapType = new TypeToken<Map<String, Object>>() {}.getType();
		Gson gson = new Gson();

		Reader json;
		try {
			// TODO: Investigate using JsonPath (https://github.com/jayway/JsonPath) to make this simpler to read/understand.
			// To test JsonPaths, go to http://jsonpath.herokuapp.com/
			json = new FileReader(filePath);
			Map<String, Object> jsonContents = gson.fromJson(json, simpleMapType);
			
			// Get normal BAM object ID
//			String pathToNormalBam= "$.normal.files[?(@.file_name=~/.*\\.bam/)].object_id";
//			String pathToNormalBai= "$.normal.files[?(@.file_name=~/.*\\.bai/)].object_id";
//			String pathToNormalMetadataURL = "$.normal.available_repos[0]";
//			String pathToNormalGNOSID = "$.normal.gnos_id";
			Configuration jsonPathConfig = Configuration.defaultConfiguration();
//			String normalBamObjectID = (String) (JsonPath.using(jsonPathConfig).parse(new File(filePath)).read(pathToNormalBam,List.class)).get(0);
//			String normalBaiObjectID = (String) (JsonPath.using(jsonPathConfig).parse(new File(filePath)).read(pathToNormalBai,List.class)).get(0);
//			String normalBamMetadataURL = (String) (JsonPath.using(jsonPathConfig).parse(new File(filePath)).read(pathToNormalMetadataURL,Map.class)).keySet().toArray()[0];
//			String normalGnosID = (String) (JsonPath.using(jsonPathConfig).parse(new File(filePath)).read(pathToNormalGNOSID,String.class));
//			normalBamMetadataURL += "cghub/metadata/analysisFull/" + normalGnosID;
			//String normalBamObjectID = JsonPath.read(json, pathToNormalBam);
//			results.put(BAM_NORMAL_OBJECT_ID, normalBamObjectID);
//			results.put(BAM_NORMAL_INDEX_OBJECT_ID, normalBaiObjectID);
//			results.put(BAM_NORMAL_METADATA_URL, normalBamMetadataURL);
			Map<String,String> normalBamInfo =  (Map<String, String>) JsonPath.using(jsonPathConfig).parse(new File(filePath)).read("$.normal.files[?(@.file_name=~/.*\\.bam/)]",List.class).get(0);
			Map<String,String> normalBaiInfo =  (Map<String, String>) JsonPath.using(jsonPathConfig).parse(new File(filePath)).read("$.normal.files[?(@.file_name=~/.*\\.bai/)]",List.class).get(0);
			String normalBamMetadataURL = (String) (JsonPath.using(jsonPathConfig).parse(new File(filePath)).read("$.normal.available_repos[0]",Map.class)).keySet().toArray()[0];
			String normalGnosID = (String) (JsonPath.using(jsonPathConfig).parse(new File(filePath)).read("$.normal.gnos_id",String.class));
			normalBamMetadataURL += "cghub/metadata/analysisFull/" + normalGnosID;
			Map<String,Object> normalInfo = new HashMap<String, Object>(4);
			normalInfo.put(FILE, normalBamInfo);
			normalInfo.put(INDEX, normalBaiInfo);
			normalInfo.put(BAM_NORMAL_METADATA_URL, normalBamMetadataURL);
			results.put(NORMAL_BAM_INFO, normalInfo);
			
			//String pathToNormalBai= "$.normal.files[file_name=~/.*\\.bai/i].object_id";
//			Map<String, Object> normal = (Map<String, Object>) jsonContents.get("normal");
//			List<Map<String, String>> normalFiles = (List<Map<String, String>>) normal.get("files");
//			for (Map<String, String> fileDetails : normalFiles) {
//				String fileName = fileDetails.get("file_name");
//				if (fileName.endsWith(".bam")) {
//					results.put(BAM_NORMAL_OBJECT_ID, fileDetails.get("object_id"));
//					List<Map<String,Object>> repos = ( (List<Map<String,Object>>) normal.get("available_repos")) ;
//					String repo_id = (String) repos.get(0).keySet().toArray()[0];
//					results.put(BAM_NORMAL_METADATA_URL, repo_id+"cghub/metadata/analysisFull/"+normal.get("gnos_id"));
//					// break the for-loop, no need to keep going through other
//					// file details.
//					break;
//				}
//			}

			// Get Tumour BAM object ID
			Map<String,String> tumourBamInfo =  (Map<String, String>) JsonPath.using(jsonPathConfig).parse(new File(filePath)).read("$.tumors[0].files[?(@.file_name=~/.*\\.bam/)]",List.class).get(0);
			Map<String,String> tumourBaiInfo =  (Map<String, String>) JsonPath.using(jsonPathConfig).parse(new File(filePath)).read("$.tumors[0].files[?(@.file_name=~/.*\\.bai/)]",List.class).get(0);
			String tumourBamMetadataURL = (String) (JsonPath.using(jsonPathConfig).parse(new File(filePath)).read("$.tumors[0].available_repos[0]",Map.class)).keySet().toArray()[0];
			String tumourGnosID = (String) (JsonPath.using(jsonPathConfig).parse(new File(filePath)).read("$.tumors[0].gnos_id",String.class));
			tumourBamMetadataURL += "cghub/metadata/analysisFull/" + tumourGnosID;
			Map<String,Object> tumourInfo = new HashMap<String, Object>(4);
			tumourInfo.put(FILE, tumourBamInfo);
			tumourInfo.put(INDEX, tumourBaiInfo);
			tumourInfo.put(BAM_TUMOUR_METADATA_URL, tumourBamMetadataURL);
			results.put(TUMOUR_BAM_INFO, tumourInfo);
//			List<Map<String, Object>> tumours = (List<Map<String, Object>>) jsonContents.get("tumors");
//			// Eventually, we will have to deal with multi-tumour sitautions
//			// properly, but for now lets just assume ONE tumour.
//			List<Map<String, String>> tumourFiles = (List<Map<String, String>>) tumours.get(0).get("files");
//			for (Map<String, String> fileDetails : tumourFiles) {
//				String fileName = fileDetails.get("file_name");
//				if (fileName.endsWith(".bam")) {
//					results.put(BAM_TUMOUR_OBJECT_ID, fileDetails.get("object_id"));
//					//TODO: This will need to be updated for multi-tumour samples.
//					List<Map<String,Object>> repos = ( (List<Map<String,Object>>) tumours.get(0).get("available_repos")) ;
//					String repo_id = (String) repos.get(0).keySet().toArray()[0];
//					results.put(BAM_TUMOUR_METADATA_URL, repo_id+"cghub/metadata/analysisFull/"+tumours.get(0).get("gnos_id"));
//					break;
//				}
//			}

			// Get the aliquot ID from the tumour. This may get more complicated
			// in multi-tumour scenarios.
			String aliquotID = (String) (JsonPath.using(jsonPathConfig).parse(new File(filePath)).read("$.tumors[0].aliquot_id",String.class)); 
			results.put(ALIQUOT_ID, aliquotID);

			// Get VCF Object IDs
			// Sanger
			Map<String,String> sangerVCFInfo = (Map<String, String>) (JsonPath.using(jsonPathConfig).parse(new File(filePath)).read("$.sanger.files[?(@.file_name=~/.*\\.somatic\\.snv_mnv\\.vcf\\.gz/)]",List.class)).get(0);
			Map<String,String> sangerVCFIndexInfo = (Map<String, String>) (JsonPath.using(jsonPathConfig).parse(new File(filePath)).read("$.sanger.files[?(@.file_name=~/(.*\\.somatic\\.snv_mnv\\.vcf\\.gz)(\\.tbi|\\.idx)/)]",List.class)).get(0);
			Map<String,Object> sangerInfo = new HashMap<String, Object>(2);
			sangerInfo.put(FILE, sangerVCFInfo);
			sangerInfo.put(INDEX, sangerVCFIndexInfo);
			//String sangerVCFIndexObjectID = (String) (JsonPath.using(jsonPathConfig).parse(new File(filePath)).read("$.sanger.files[?(@.file_name=~/(.*\\.somatic\\.snv_mnv\\.vcf\\.gz)(\\.tbi|\\.idx)/)].object_id",List.class)).get(0);
			results.put(SANGER_VCF_INFO, sangerInfo);
//			Map<String, Object> sanger = (Map<String, Object>) jsonContents.get("sanger");
//			List<Map<String, String>> sangerFiles = (List<Map<String, String>>) sanger.get("files");
//			for (Map<String, String> fileDetails : sangerFiles) {
//				String fileName = fileDetails.get("file_name");
//				// TODO: We will probably have to download ALL VCFs and then do
//				// the merge, like Brian was suggesting, so we'll actually need
//				// object IDs for suffixes: ".somatic.snv_mnv.vcf.gz",
//				// ".somatic.sv.vcf.gz", ".somatic.indel.vcf.gz"
//				if (fileName.endsWith(".somatic.snv_mnv.vcf.gz")) {
//					// this.sangerVCFObjectID = fileDetails.get("object_id");
//					results.put(SANGER_VCF_OBJECT_ID, fileDetails.get("object_id"));
//					// break the for-loop, no need to keep going through other
//					// file details.
//					break;
//				}
//			}
			// DFKZ-EMBL
			Map<String, Object> dkfz = (Map<String, Object>) jsonContents.get("dkfz_embl");
			List<Map<String, String>> dkfzFiles = (List<Map<String, String>>) dkfz.get("files");
			for (Map<String, String> fileDetails : dkfzFiles) {
				String fileName = fileDetails.get("file_name");
				// TODO: Get the other VCF files. Also, DKFZ needs to be
				// filtered to not use the "embl-delly" files.
				if (fileName.contains("dkfz-snvCalling") && fileName.endsWith(".somatic.snv_mnv.vcf.gz")) {
					results.put(DKFZEMBL_VCF_OBJECT_ID, fileDetails.get("object_id"));
					break;
				}
			}
			// Broad
			Map<String, Object> broad = (Map<String, Object>) jsonContents.get("broad");
			List<Map<String, String>> broadFiles = (List<Map<String, String>>) broad.get("files");
			for (Map<String, String> fileDetails : broadFiles) {
				String fileName = fileDetails.get("file_name");
				if (fileName.contains("broad-mutect") && fileName.endsWith(".somatic.snv_mnv.vcf.gz")) {
					results.put(BROAD_VCF_OBJECT_ID, fileDetails.get("object_id"));
					break;
				}
			}
			// Muse
			Map<String, Object> muse = (Map<String, Object>) jsonContents.get("muse");
			List<Map<String, String>> museFiles = (List<Map<String, String>>) muse.get("files");
			for (Map<String, String> fileDetails : museFiles) {
				String fileName = fileDetails.get("file_name");
				if (fileName.contains("MUSE_1-0rc-vcf") && fileName.endsWith(".somatic.snv_mnv.vcf.gz")) {
					results.put(MUSE_VCF_OBJECT_ID, fileDetails.get("object_id"));
					break;
				}
			}
			
			// Get OxoQ Score
			//String oxoqScore = (String) tumours.get(0).get("oxog_score");
			String oxoqScore = (String) (JsonPath.using(jsonPathConfig).parse(new File(filePath)).read("$.tumors[0].oxog_score",String.class));
			results.put(OXOQ_SCORE, oxoqScore);
			
			// Get donor ID
			String submitterDonorID = (String) jsonContents.get("submitter_donor_id");
			results.put(SUBMITTER_DONOR_ID, submitterDonorID);

			// Get project code
			String projectCode = (String) jsonContents.get("project_code");
			results.put(PROJECT_CODE, projectCode);
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return results;

	}
}
