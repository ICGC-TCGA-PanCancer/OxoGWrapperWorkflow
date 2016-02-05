package com.github.seqware;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.seqware.OxoGWrapperWorkflow.VCFType;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

public abstract class JSONUtils {

	static final String OXOQ_SCORE = "OxoQScore";

	static final String BROAD_SNV_VCF_OBJECT_ID = "broad_snv_data_object_id";
	static final String BROAD_SNV_VCF_NAME = "broad_snv_data_file_name";
	static final String DKFZEMBL_SNV_VCF_OBJECT_ID = "dkfz_embl_snv_data_object_id";
	static final String DKFZEMBL_SNV_VCF_NAME = "dkfz_embl_snv_data_file_name";
	static final String SANGER_SNV_VCF_OBJECT_ID = "sanger_snv_data_object_id";
	static final String SANGER_SNV_VCF_NAME = "sanger_snv_data_file_name";
	static final String MUSE_VCF_OBJECT_ID = "muse_snv_data_object_id";
	static final String MUSE_VCF_NAME = "muse_snv_data_file_name";

	static final String BROAD_SV_VCF_OBJECT_ID = "broad_sv_data_object_id";
	static final String BROAD_SV_VCF_NAME = "broad_sv_data_file_name";
	static final String DKFZEMBL_SV_VCF_OBJECT_ID = "dkfz_embl_sv_data_object_id";
	static final String DKFZEMBL_SV_VCF_NAME = "dkfz_embl_sv_data_file_name";
	static final String SANGER_SV_VCF_OBJECT_ID = "sanger_sv_data_object_id";
	static final String SANGER_SV_VCF_NAME = "sanger_sv_data_file_name";

	static final String BROAD_INDEL_VCF_OBJECT_ID = "broad_indel_data_object_id";
	static final String BROAD_INDEL_VCF_NAME = "broad_indel_data_file_name";
	static final String DKFZEMBL_INDEL_VCF_OBJECT_ID = "dkfz_embl_indel_data_object_id";
	static final String DKFZEMBL_INDEL_VCF_NAME = "dkfz_embl_indel_data_file_name";
	static final String SANGER_INDEL_VCF_OBJECT_ID = "sanger_indel_data_object_id";
	static final String SANGER_INDEL_VCF_NAME = "sanger_indel_data_file_name";

	static final String SANGER_SNV_INDEX_OBJECT_ID = "sanger_snv_index_object_id";
	static final String BROAD_SNV_INDEX_OBJECT_ID = "broad_snv_index_object_id";
	static final String DKFZEMBL_SNV_INDEX_OBJECT_ID = "dkfz_embl_snv_index_object_id";
	static final String MUSE_SNV_INDEX_OBJECT_ID = "muse_snv_index_object_id";

	static final String SANGER_SV_INDEX_OBJECT_ID = "sanger_sv_index_object_id";
	static final String BROAD_SV_INDEX_OBJECT_ID = "broad_sv_index_object_id";
	static final String DKFZEMBL_SV_INDEX_OBJECT_ID = "dkfz_embl_sv_index_object_id";

	static final String SANGER_INDEL_INDEX_OBJECT_ID = "sanger_indel_index_object_id";
	static final String BROAD_INDEL_INDEX_OBJECT_ID = "broad_indel_index_object_id";
	static final String DKFZEMBL_INDEL_INDEX_OBJECT_ID = "dkfz_embl_indel_index_object_id";

	static final String BAM_NORMAL_OBJECT_ID = "normal_data_object_id";
	static final String BAM_NORMAL_FILE_NAME = "normal_data_file_name";
	static final String BAM_NORMAL_INDEX_OBJECT_ID = "normal_index_object_id";
	static final String BAM_NORMAL_METADATA_URL = "bamNormalMetadataURL";
	static final String BAM_NORMAL_GNOS_ID = "normal_gnos_id";

	static final String BAM_TUMOUR_OBJECT_ID = "tumour_data_object_id";
	static final String BAM_TUMOUR_FILE_NAME = "tumour_data_file_name";
	static final String BAM_TUMOUR_INDEX_OBJECT_ID = "tumour_index_object_id";
	static final String BAM_TUMOUR_METADATA_URL = "bamTumourMetadataURL";
	static final String BAM_TUMOUR_GNOS_ID = "tumour_gnos_id";

	static final String BROAD_GNOS_ID = "broad_gnosID";
	static final String SANGER_GNOS_ID = "sanger_gnosID";
	static final String DKFZEMBL_GNOS_ID = "dkfz_embl_gnosID";
	static final String MUSE_GNOS_ID = "muse_gnosID";

	static final String ALIQUOT_ID = "aliquotID";
	static final String SUBMITTER_DONOR_ID = "submitterDonorID";
	static final String PROJECT_CODE = "projectCode";

	static final String GNOS_ID = "gnosID";
	static final String DATA = "data";
	static final String INDEX = "index";
	static final String TAG = "tag";
	static final String FILE_TYPE = "filetype";

	static final String NORMAL_BAM_INFO = "normalBamInfo";
	static final String TUMOUR_BAM_INFO = "tumourBamInfo";
	static final String SANGER_VCF_INFO = "sangerVcfInfo";
	static final String DKFZEMBL_VCF_INFO = "dkfzemblVcfInfo";
	static final String BROAD_VCF_INFO = "broadVcfInfo";
	static final String MUSE_VCF_INFO = "museVcfInfo";

	@SuppressWarnings("unchecked")
	private static Map<String, Object> extractFileInfo(File jsonFile, Configuration jsonPathConfig,
			String workflowNameInJson) throws IOException {

		DocumentContext parsedJSON = JsonPath.using(jsonPathConfig).parse(jsonFile);
		Map<String, Object> info = new HashMap<String, Object>(4);
		// SNV

		Map<String, String> snvVCFInfo = (Map<String, String>) (parsedJSON.read(
				"$." + workflowNameInJson + ".files[?(@.file_name=~/.*\\.somatic\\.snv_mnv\\.vcf\\.gz/)]", List.class))
						.get(0);
		Map<String, String> snvVCFIndexInfo = (Map<String, String>) (parsedJSON.read("$." + workflowNameInJson
				+ ".files[?(@.file_name=~/(.*\\.somatic\\.snv_mnv\\.vcf\\.gz)(\\.tbi|\\.idx)/)]", List.class)).get(0);

		// get GNOS ID
		String gnosID = (String) (parsedJSON.read("$." + workflowNameInJson + ".gnos_id", String.class));

		if (!workflowNameInJson.equals("muse")) {
			// INDEL
			Map<String, String> indelVCFInfo = (Map<String, String>) (parsedJSON.read(
					"$." + workflowNameInJson + ".files[?(@.file_name=~/.*\\.somatic\\.indel\\.vcf\\.gz/)]",
					List.class)).get(0);
			Map<String, String> indelVCFIndexInfo = (Map<String, String>) (parsedJSON.read("$." + workflowNameInJson
					+ ".files[?(@.file_name=~/(.*\\.somatic\\.indel\\.vcf\\.gz)(\\.tbi|\\.idx)/)]", List.class)).get(0);
			// SV
			Map<String, String> svVCFInfo = (Map<String, String>) (parsedJSON.read(
					"$." + workflowNameInJson + ".files[?(@.file_name=~/.*\\.somatic\\.sv\\.vcf\\.gz/)]", List.class))
							.get(0);
			Map<String, String> svVCFIndexInfo = (Map<String, String>) (parsedJSON.read("$." + workflowNameInJson
					+ ".files[?(@.file_name=~/(.*\\.somatic\\.sv\\.vcf\\.gz)(\\.tbi|\\.idx)/)]", List.class)).get(0);

			Map<String, Object> indelInfo = new HashMap<String, Object>(2);
			indelInfo.put(DATA, indelVCFInfo);
			indelInfo.put(INDEX, indelVCFIndexInfo);
			info.put(VCFType.indel.toString(), indelInfo);
			Map<String, Object> svInfo = new HashMap<String, Object>(2);
			svInfo.put(DATA, svVCFInfo);
			svInfo.put(INDEX, svVCFIndexInfo);
			info.put(VCFType.sv.toString(), svInfo);

		}
		Map<String, Object> fileInfo = new HashMap<String, Object>(2);
		fileInfo.put(DATA, snvVCFInfo);
		fileInfo.put(INDEX, snvVCFIndexInfo);
		info.put(VCFType.snv.toString(), fileInfo);
		info.put(TAG, workflowNameInJson);
		info.put(GNOS_ID, gnosID);

		return info;
	}

	@SuppressWarnings("unchecked")
	public static Map<String, Object> processJSONFile(String filePath) {

		Map<String, Object> results = new HashMap<String, Object>(8);

		try {
			// Now using JsonPath
			// (https://github.com/jayway/JsonPath) to make this simpler to
			// read/understand.
			// To test JsonPaths, go to http://jsonpath.herokuapp.com/

			Configuration jsonPathConfig = Configuration.defaultConfiguration();

			// Get normal BAM object ID
			File jsonFile = new File(filePath);
			DocumentContext parsedJSON = JsonPath.using(jsonPathConfig).parse(jsonFile);
			Map<String, String> normalBamInfo = (Map<String, String>) parsedJSON
					.read("$.normal.files[?(@.file_name=~/.*\\.bam/)]", List.class).get(0);
			Map<String, String> normalBaiInfo = (Map<String, String>) parsedJSON
					.read("$.normal.files[?(@.file_name=~/.*\\.bai/)]", List.class).get(0);
			String normalBamMetadataURL = (String) (parsedJSON.read("$.normal.available_repos[0]", Map.class)).keySet()
					.toArray()[0];
			String normalGnosID = (String) (parsedJSON.read("$.normal.gnos_id", String.class));
			normalBamMetadataURL += "cghub/metadata/analysisFull/" + normalGnosID;
			Map<String, Object> normalInfo = new HashMap<String, Object>(4);
			normalInfo.put(DATA, normalBamInfo);
			normalInfo.put(INDEX, normalBaiInfo);
			results.put(BAM_NORMAL_METADATA_URL, normalBamMetadataURL);
			normalInfo.put(TAG, "normal");
			results.put(NORMAL_BAM_INFO, normalInfo);
			results.put(BAM_NORMAL_GNOS_ID, normalGnosID);

			// Get Tumour BAM object ID
			Map<String, String> tumourBamInfo = (Map<String, String>) parsedJSON
					.read("$.tumors[0].files[?(@.file_name=~/.*\\.bam/)]", List.class).get(0);
			Map<String, String> tumourBaiInfo = (Map<String, String>) parsedJSON
					.read("$.tumors[0].files[?(@.file_name=~/.*\\.bai/)]", List.class).get(0);
			String tumourBamMetadataURL = (String) (parsedJSON.read("$.tumors[0].available_repos[0]", Map.class))
					.keySet().toArray()[0];
			String tumourGnosID = (String) (parsedJSON.read("$.tumors[0].gnos_id", String.class));
			tumourBamMetadataURL += "cghub/metadata/analysisFull/" + tumourGnosID;
			Map<String, Object> tumourInfo = new HashMap<String, Object>(4);
			tumourInfo.put(DATA, tumourBamInfo);
			tumourInfo.put(INDEX, tumourBaiInfo);
			results.put(BAM_TUMOUR_METADATA_URL, tumourBamMetadataURL);
			tumourInfo.put(TAG, "tumour");
			results.put(TUMOUR_BAM_INFO, tumourInfo);
			results.put(BAM_TUMOUR_GNOS_ID, tumourGnosID);

			// Get the aliquot ID from the tumour. This may get more complicated
			// in multi-tumour scenarios.
			String aliquotID = (String) (parsedJSON.read("$.tumors[0].aliquot_id", String.class));
			results.put(ALIQUOT_ID, aliquotID);

			// Sanger
			Map<String, Object> sangerInfo = extractFileInfo(jsonFile, jsonPathConfig, "sanger");
			results.put(SANGER_VCF_INFO, sangerInfo);

			// DKFZ-EMBL
			Map<String, Object> dkfzemblInfo = extractFileInfo(jsonFile, jsonPathConfig, "dkfz_embl");
			results.put(DKFZEMBL_VCF_INFO, dkfzemblInfo);

			// Broad
			Map<String, Object> broadInfo = extractFileInfo(jsonFile, jsonPathConfig, "broad");
			results.put(BROAD_VCF_INFO, broadInfo);

			// Muse
			Map<String, Object> museInfo = extractFileInfo(jsonFile, jsonPathConfig, "muse");
			results.put(MUSE_VCF_INFO, museInfo);

			// Get OxoQ Score
			String oxoqScore = (String) (parsedJSON.read("$.tumors[0].oxog_score", String.class));
			results.put(OXOQ_SCORE, oxoqScore);

			// Get donor ID
			String submitterDonorID = (String) (parsedJSON.read("$.submitter_donor_id", String.class));
			results.put(SUBMITTER_DONOR_ID, submitterDonorID);

			// Get project code
			String projectCode = (String) (parsedJSON.read("$.project_code", String.class));
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
