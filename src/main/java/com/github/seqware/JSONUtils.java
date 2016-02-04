package com.github.seqware;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;

public abstract class JSONUtils {

	static final String OXOQ_SCORE = "OxoQScore";
	static final String BROAD_VCF_OBJECT_ID = "broad_data_object_id";
	static final String DKFZEMBL_VCF_OBJECT_ID = "dkfz_embl_data_object_id";
	static final String SANGER_VCF_OBJECT_ID = "sanger_data_object_id";
	static final String BAM_NORMAL_OBJECT_ID = "normal_data_object_id";
	static final String BAM_NORMAL_INDEX_OBJECT_ID = "normal_index_object_id";
	static final String BAM_TUMOUR_INDEX_OBJECT_ID = "bamTumourBaiObjectID";
	static final String BAM_NORMAL_METADATA_URL = "tumour_index_object_id";
	static final String BAM_TUMOUR_OBJECT_ID = "tumour_data_object_id";
	static final String BAM_TUMOUR_METADATA_URL = "bamTumourMetadataURL";
	static final String MUSE_VCF_OBJECT_ID = "muse_data_object_id";
	static final String ALIQUOT_ID = "aliquotID";
	static final String SUBMITTER_DONOR_ID = "submitterDonorID";
	static final String PROJECT_CODE = "projectCode";

	static final String DATA = "data";
	static final String INDEX = "index";
	static final String TAG = "tag";

	static final String NORMAL_BAM_INFO = "normalBamInfo";
	static final String TUMOUR_BAM_INFO = "tumourBamInfo";
	static final String SANGER_VCF_INFO = "sangerVcfInfo";
	static final String DKFZEMBL_VCF_INFO = "dkfzemblVcfInfo";
	static final String BROAD_VCF_INFO = "broadVcfInfo";
	static final String MUSE_VCF_INFO = "museVcfInfo";

	public static Map<String, Object> processJSONFile(String filePath) {

		Map<String, Object> results = new HashMap<String, Object>(8);

		// Type simpleMapType = new TypeToken<Map<String, Object>>()
		// {}.getType();
		// Gson gson = new Gson();

		Reader json;
		try {
			// TODO: Investigate using JsonPath
			// (https://github.com/jayway/JsonPath) to make this simpler to
			// read/understand.
			// To test JsonPaths, go to http://jsonpath.herokuapp.com/
			json = new FileReader(filePath);

			Configuration jsonPathConfig = Configuration.defaultConfiguration();

			// Get normal BAM object ID
			Map<String, String> normalBamInfo = (Map<String, String>) JsonPath.using(jsonPathConfig)
					.parse(new File(filePath)).read("$.normal.files[?(@.file_name=~/.*\\.bam/)]", List.class).get(0);
			Map<String, String> normalBaiInfo = (Map<String, String>) JsonPath.using(jsonPathConfig)
					.parse(new File(filePath)).read("$.normal.files[?(@.file_name=~/.*\\.bai/)]", List.class).get(0);
			String normalBamMetadataURL = (String) (JsonPath.using(jsonPathConfig).parse(new File(filePath))
					.read("$.normal.available_repos[0]", Map.class)).keySet().toArray()[0];
			String normalGnosID = (String) (JsonPath.using(jsonPathConfig).parse(new File(filePath))
					.read("$.normal.gnos_id", String.class));
			normalBamMetadataURL += "cghub/metadata/analysisFull/" + normalGnosID;
			Map<String, Object> normalInfo = new HashMap<String, Object>(4);
			normalInfo.put(DATA, normalBamInfo);
			normalInfo.put(INDEX, normalBaiInfo);
			results.put(BAM_NORMAL_METADATA_URL, normalBamMetadataURL);
			normalInfo.put(TAG, "normal");
			results.put(NORMAL_BAM_INFO, normalInfo);

			// Get Tumour BAM object ID
			Map<String, String> tumourBamInfo = (Map<String, String>) JsonPath.using(jsonPathConfig)
					.parse(new File(filePath)).read("$.tumors[0].files[?(@.file_name=~/.*\\.bam/)]", List.class).get(0);
			Map<String, String> tumourBaiInfo = (Map<String, String>) JsonPath.using(jsonPathConfig)
					.parse(new File(filePath)).read("$.tumors[0].files[?(@.file_name=~/.*\\.bai/)]", List.class).get(0);
			String tumourBamMetadataURL = (String) (JsonPath.using(jsonPathConfig).parse(new File(filePath))
					.read("$.tumors[0].available_repos[0]", Map.class)).keySet().toArray()[0];
			String tumourGnosID = (String) (JsonPath.using(jsonPathConfig).parse(new File(filePath))
					.read("$.tumors[0].gnos_id", String.class));
			tumourBamMetadataURL += "cghub/metadata/analysisFull/" + tumourGnosID;
			Map<String, Object> tumourInfo = new HashMap<String, Object>(4);
			tumourInfo.put(DATA, tumourBamInfo);
			tumourInfo.put(INDEX, tumourBaiInfo);
			results.put(BAM_TUMOUR_METADATA_URL, tumourBamMetadataURL);
			tumourInfo.put(TAG, "tumour");
			results.put(TUMOUR_BAM_INFO, tumourInfo);

			// Get the aliquot ID from the tumour. This may get more complicated
			// in multi-tumour scenarios.
			String aliquotID = (String) (JsonPath.using(jsonPathConfig).parse(new File(filePath))
					.read("$.tumors[0].aliquot_id", String.class));
			results.put(ALIQUOT_ID, aliquotID);

			// Get VCF Object IDs
			// Sanger
			Map<String, String> sangerVCFInfo = (Map<String, String>) (JsonPath.using(jsonPathConfig)
					.parse(new File(filePath))
					.read("$.sanger.files[?(@.file_name=~/.*\\.somatic\\.snv_mnv\\.vcf\\.gz/)]", List.class)).get(0);
			Map<String, String> sangerVCFIndexInfo = (Map<String, String>) (JsonPath.using(jsonPathConfig)
					.parse(new File(filePath))
					.read("$.sanger.files[?(@.file_name=~/(.*\\.somatic\\.snv_mnv\\.vcf\\.gz)(\\.tbi|\\.idx)/)]",
							List.class)).get(0);
			Map<String, Object> sangerInfo = new HashMap<String, Object>(2);
			sangerInfo.put(DATA, sangerVCFInfo);
			sangerInfo.put(INDEX, sangerVCFIndexInfo);
			sangerInfo.put(TAG, "sanger");
			results.put(SANGER_VCF_INFO, sangerInfo);

			// DFKZ-EMBL
			Map<String, String> dkfzemblVCFInfo = (Map<String, String>) (JsonPath.using(jsonPathConfig)
					.parse(new File(filePath))
					.read("$.dkfz_embl.files[?(@.file_name=~/.*\\.somatic\\.snv_mnv\\.vcf\\.gz/)]", List.class)).get(0);
			Map<String, String> dkfzemblVCFIndexInfo = (Map<String, String>) (JsonPath.using(jsonPathConfig)
					.parse(new File(filePath))
					.read("$.dkfz_embl.files[?(@.file_name=~/(.*\\.somatic\\.snv_mnv\\.vcf\\.gz)(\\.tbi|\\.idx)/)]",
							List.class)).get(0);
			Map<String, Object> dkfzemblInfo = new HashMap<String, Object>(2);
			dkfzemblInfo.put(DATA, dkfzemblVCFInfo);
			dkfzemblInfo.put(INDEX, dkfzemblVCFIndexInfo);
			dkfzemblInfo.put(TAG, "dkfz_embl");
			results.put(DKFZEMBL_VCF_INFO, dkfzemblInfo);

			// Broad
			Map<String, String> broadVCFInfo = (Map<String, String>) (JsonPath.using(jsonPathConfig)
					.parse(new File(filePath))
					.read("$.broad.files[?(@.file_name=~/.*\\.somatic\\.snv_mnv\\.vcf\\.gz/)]", List.class)).get(0);
			Map<String, String> broadVCFIndexInfo = (Map<String, String>) (JsonPath.using(jsonPathConfig)
					.parse(new File(filePath))
					.read("$.broad.files[?(@.file_name=~/(.*\\.somatic\\.snv_mnv\\.vcf\\.gz)(\\.tbi|\\.idx)/)]",
							List.class)).get(0);
			Map<String, Object> broadInfo = new HashMap<String, Object>(2);
			broadInfo.put(DATA, broadVCFInfo);
			broadInfo.put(INDEX, broadVCFIndexInfo);
			broadInfo.put(TAG, "broad");
			results.put(BROAD_VCF_INFO, broadInfo);

			// Muse
			Map<String, String> museVCFInfo = (Map<String, String>) (JsonPath.using(jsonPathConfig)
					.parse(new File(filePath))
					.read("$.muse.files[?(@.file_name=~/.*\\.somatic\\.snv_mnv\\.vcf\\.gz/)]", List.class)).get(0);
			Map<String, String> museVCFIndexInfo = (Map<String, String>) (JsonPath.using(jsonPathConfig)
					.parse(new File(filePath))
					.read("$.muse.files[?(@.file_name=~/(.*\\.somatic\\.snv_mnv\\.vcf\\.gz)(\\.tbi|\\.idx)/)]",
							List.class)).get(0);
			Map<String, Object> museInfo = new HashMap<String, Object>(2);
			museInfo.put(DATA, museVCFInfo);
			museInfo.put(INDEX, museVCFIndexInfo);
			museInfo.put(TAG, "muse");
			results.put(MUSE_VCF_INFO, museInfo);

			// Get OxoQ Score
			String oxoqScore = (String) (JsonPath.using(jsonPathConfig).parse(new File(filePath))
					.read("$.tumors[0].oxog_score", String.class));
			results.put(OXOQ_SCORE, oxoqScore);

			// Get donor ID
			String submitterDonorID = (String) (JsonPath.using(jsonPathConfig).parse(new File(filePath))
					.read("$.submitter_donor_id", String.class));
			results.put(SUBMITTER_DONOR_ID, submitterDonorID);

			// Get project code
			String projectCode = (String) (JsonPath.using(jsonPathConfig).parse(new File(filePath))
					.read("$.project_code", String.class));
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
