package com.github.seqware;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.github.seqware.OxoGWrapperWorkflow.BAMType;
import com.github.seqware.OxoGWrapperWorkflow.Pipeline;
import com.github.seqware.OxoGWrapperWorkflow.VCFType;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

public abstract class JSONUtils {

	public static String lookUpKeyGenerator(Pipeline p, VCFType v, String dataOrIndex, String keyType)
	{
		return p.toString()+"_"+v.toString()+"_"+dataOrIndex+"_"+keyType;
	}
	
	public static String lookUpKeyGenerator(BAMType b, VCFType v, String dataOrIndex, String keyType)
	{
		return b.toString()+"_"+v.toString()+"_"+dataOrIndex+"_"+keyType;
	}

	
	static final String OXOQ_SCORE = "OxoQScore";

	// ugh... so many constants. There's probably a more elegant way to do this,
	// seeing as most of them follow a pattern. Just don't have time to fix that
	// right now.
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
	static final String SUBMITTER_SPECIMENT_ID = "submitterSpecimenID";
	static final String PROJECT_CODE = "projectCode";

	static final String GNOS_ID = "gnosID";
	static final String DATA = "data";
	static final String INDEX = "index";
	static final String TAG = "tag";
	static final String NUMBER = "number";
	static final String FILE_TYPE = "filetype";

	static final String NORMAL_BAM_INFO = "normalBamInfo";
	static final String TUMOUR_BAM_INFO = "tumourBamInfo";
	static final String SANGER_VCF_INFO = "sangerVcfInfo";
	static final String DKFZEMBL_VCF_INFO = "dkfzemblVcfInfo";
	static final String BROAD_VCF_INFO = "broadVcfInfo";
	static final String MUSE_VCF_INFO = "museVcfInfo";
	
	// The URLS must end up looking like this:
	// https://gtrepo-ebi.annailabs.com/cghub/data/analysis/download/96e252b8-911a-44c7-abc6-b924845e0be6
	// They are of the form ${available_repos[0]}/cghub/data/analysis/download/${GNOS_id}
	static final String SANGER_DOWNLOAD_URL = "sanger_download_url";
	static final String BROAD_DOWNLOAD_URL = "broad_download_url";
	static final String DKFZ_EMBL_DOWNLOAD_URL = "dkfz_embl_download_url";
	static final String MUSE_DOWNLOAD_URL = "muse_download_url";
	static final String NORMAL_BAM_DOWNLOAD_URL = "normal_bam_download_url";
	static final String TUMOUR_BAM_DOWNLOAD_URL = "tumour_bam_download_url";
	
	static final String NORMAL_BAM_INDEX_FILE_NAME = "normal_index_file_name";
	static final String TUMOUR_BAM_INDEX_FILE_NAME = "tumour_index_file_name";
	static final String SANGER_SNV_INDEX_FILE_NAME = "sanger_snv_index_file_name";
	static final String SANGER_SV_INDEX_FILE_NAME = "sanger_sv_index_file_name";
	static final String SANGER_INDEL_INDEX_FILE_NAME = "sanger_indel_index_file_name";
	static final String BROAD_SNV_INDEX_FILE_NAME = "broad_snv_index_file_name";
	static final String BROAD_SV_INDEX_FILE_NAME = "broad_sv_index_file_name";
	static final String BROAD_INDEL_INDEX_FILE_NAME = "broad_indel_index_file_name";
	static final String DKFZ_EMBL_SNV_INDEX_FILE_NAME = "dkfz_embl_snv_index_file_name";
	static final String DKFZ_EMBL_SV_INDEX_FILE_NAME = "dkfz_embl_sv_index_file_name";
	static final String DKFZ_EMBL_INDEL_INDEX_FILE_NAME = "dkfz_embl_indel_index_file_name";
	static final String MUSE_SNV_INDEX_FILE_NAME = "muse_snv_index_file_name";
	static final String TUMOUR_COUNT = "tumourCount";
	static final String TUMOUR_ALIQUOT_ID = "tumour_aliquot_id";

	private static String extractRepoInfo(File jsonFile, Configuration jsonPathConfig, String workflowNameInJson) throws IOException
	{
		String repoURL = "";

		DocumentContext parsedJSON = JsonPath.using(jsonPathConfig).parse(jsonFile);
		String path = "$.."+workflowNameInJson+".gnos_repo[0]";
		@SuppressWarnings("unchecked") List<String> repoInfo = (List<String>) parsedJSON.read(path, List.class);
		//There should only be one element anyway. Multitumour will have to be handled differently.
		repoURL = ((List<String>)repoInfo).get(0);
		path = "$.."+workflowNameInJson+".gnos_id";
		@SuppressWarnings("unchecked") List<String> gnosIDInfo = (List<String>) parsedJSON.read(path, List.class);
		String gnosID = ((List<String>)gnosIDInfo).get(0);
		repoURL += "cghub/data/analysis/download/"+gnosID;
		
		return repoURL;
	}
	
	@SuppressWarnings("unchecked")
	private static Map<String, Object> extractFileInfo(File jsonFile, Configuration jsonPathConfig, String workflowNameInJson, String tumourAliquotID, int index) throws IOException {
		
		Function<? super Entry<String, String>, ? extends String> keyMapper = e->e.getKey()+"_"+String.valueOf(index);
		
//		Comparator<Entry<String,String>> comparator = new Comparator<Entry<String,String>>() {
//
//			@Override
//			public int compare(Entry<String,String> o1, Entry<String,String> o2) {
//				return o1.getKey().compareTo(o2.getKey());
//			}
//		};

		
		DocumentContext parsedJSON = JsonPath.using(jsonPathConfig).parse(jsonFile);
		
		Map<String, Object> info = new TreeMap<String, Object>();
		// SNV
		Map<String, String> snvVCFInfo = ((Map<String, String>) (parsedJSON.read("$." + workflowNameInJson + ".files[?(@.file_name=~/"+tumourAliquotID+".*\\.somatic\\.snv_mnv\\.vcf\\.gz/)]", List.class)).get(0))
																	.entrySet()
																	.stream()
																	.collect(Collectors.toMap( keyMapper, Entry::getValue ) );
		Map<String, String> snvVCFIndexInfo = ((Map<String, String>) (parsedJSON.read("$." + workflowNameInJson+ ".files[?(@.file_name=~/("+tumourAliquotID+".*\\.somatic\\.snv_mnv\\.vcf\\.gz)(\\.tbi|\\.idx)/)]", List.class)).get(0))
																	.entrySet()
																	.stream()
																	.collect(Collectors.toMap( keyMapper, Entry::getValue ) );
		// get GNOS ID
		String gnosID = (String) (parsedJSON.read("$." + workflowNameInJson + ".gnos_id", String.class));

		if (!workflowNameInJson.equals("muse")) {
			// INDEL
			Map<String, String> indelVCFInfo = ((Map<String, String>) (parsedJSON.read("$." + workflowNameInJson + ".files[?(@.file_name=~/"+tumourAliquotID+".*\\.somatic\\.indel\\.vcf\\.gz/)]",List.class)).get(0))
																			.entrySet()
																			.stream()
																			.collect(Collectors.toMap( keyMapper, Entry::getValue ) );
			
			Map<String, String> indelVCFIndexInfo = ((Map<String, String>) (parsedJSON.read("$." + workflowNameInJson + ".files[?(@.file_name=~/("+tumourAliquotID+".*\\.somatic\\.indel\\.vcf\\.gz)(\\.tbi|\\.idx)/)]", List.class)).get(0))
																			.entrySet()
																			.stream()
																			.collect(Collectors.toMap( keyMapper, Entry::getValue ) );
			// SV
			Map<String, String> svVCFInfo = ((Map<String, String>) (parsedJSON.read("$." + workflowNameInJson + ".files[?(@.file_name=~/"+tumourAliquotID+".*\\.somatic\\.sv\\.vcf\\.gz/)]", List.class)).get(0))
																			.entrySet()
																			.stream()
																			.collect(Collectors.toMap( keyMapper, Entry::getValue ) );
			Map<String, String> svVCFIndexInfo = ((Map<String, String>) (parsedJSON.read("$." + workflowNameInJson + ".files[?(@.file_name=~/("+tumourAliquotID+".*\\.somatic\\.sv\\.vcf\\.gz)(\\.tbi|\\.idx)/)]", List.class)).get(0))
																			.entrySet()
																			.stream()
																			.collect(Collectors.toMap( keyMapper, Entry::getValue ) );

			Map<String, Object> indelInfo = new TreeMap<String, Object>();
			indelInfo.put(DATA, indelVCFInfo);
			indelInfo.put(INDEX, indelVCFIndexInfo);
			indelInfo.put(NUMBER, index);
			indelInfo.put(TAG, "indel");
			info.put(VCFType.indel.toString()+"_"+index, indelInfo);
			Map<String, Object> svInfo = new TreeMap<String, Object>();
			svInfo.put(DATA, svVCFInfo);
			svInfo.put(INDEX, svVCFIndexInfo);
			svInfo.put(NUMBER, index);
			svInfo.put(TAG, "sv");
			info.put(VCFType.sv.toString()+"_"+index, svInfo);

		}
		Map<String, Object> snvInfo = new TreeMap<String, Object>();
		snvInfo.put(DATA, snvVCFInfo);
		snvInfo.put(INDEX, snvVCFIndexInfo);
		snvInfo.put(NUMBER, index);
		snvInfo.put(TAG, "snv");
		info.put(VCFType.snv.toString()+"_"+index, snvInfo);
		info.put(TAG, workflowNameInJson);
		info.put(GNOS_ID, gnosID);

		return info;
	}

	@SuppressWarnings("unchecked")
	public static Map<String, Object> processJSONFile(String filePath) {

		Map<String, Object> results = new TreeMap<String, Object>();

		try {
			// Now using JsonPath
			// (https://github.com/jayway/JsonPath) to make this simpler to
			// read/understand.
			// To test JsonPaths, go to http://jsonpath.herokuapp.com/

			Configuration jsonPathConfig = Configuration.defaultConfiguration();

			// Get normal BAM object ID
			File jsonFile = new File(filePath);
			DocumentContext parsedJSON = JsonPath.using(jsonPathConfig).parse(jsonFile);
			Map<String, String> normalBamInfo = (Map<String, String>) parsedJSON.read("$.normal.files[?(@.file_name=~/.*\\.bam/)]", List.class).get(0);
			Map<String, String> normalBaiInfo = (Map<String, String>) parsedJSON.read("$.normal.files[?(@.file_name=~/.*\\.bai/)]", List.class).get(0);
			String normalBamMetadataURL = (String) (parsedJSON.read("$.normal.available_repos[0]", Map.class)).keySet().toArray()[0];
			String normalGnosID = (String) (parsedJSON.read("$.normal.gnos_id", String.class));
			normalBamMetadataURL += "cghub/metadata/analysisFull/" + normalGnosID;
			Map<String, Object> normalInfo = new TreeMap<String, Object>();
			normalInfo.put(DATA, normalBamInfo);
			normalInfo.put(INDEX, normalBaiInfo);
			results.put(BAM_NORMAL_METADATA_URL, normalBamMetadataURL);
			normalInfo.put(TAG, "normal");
			results.put(NORMAL_BAM_INFO, normalInfo);
			results.put(BAM_NORMAL_GNOS_ID, normalGnosID);

			// Get Tumour BAM object IDs
			List<Map<String,Object>> tumours = (List<Map<String,Object>>) parsedJSON.read("$.tumors", List.class);
			results.put(TUMOUR_COUNT, tumours.size() );
			for (int i = 0 ; i < tumours.size(); i++ )
			{
				final String iStr = String.valueOf(i);
				Map<String,Object> tumour = tumours.get(i);
				List<Map<String,String>> files = (List<Map<String,String>>)(tumour.get("files"));

				//Extract the submaps that contain info for bam/bai files.
				Map<String, String> tumourBamInfo = files.stream().filter(p -> p.get("file_name").endsWith(".bam")).findFirst().get();
				Map<String, String> tumourBaiInfo = files.stream().filter(p -> p.get("file_name").endsWith(".bai")).findFirst().get();
				
				//Now, inject the index into the keys of the maps, so that it comes out in the INI file as "tumour_file_name_1=...", "tumour_index_file_name_2=...", etc...
				Function<? super Entry<String, String>, ? extends String> keyUpdater = km -> km.getKey() + "_" + iStr;
				Function<? super Entry<String, String>, ? extends String> valueMapper = vm -> String.valueOf(vm.getValue());
				
				tumourBamInfo = tumourBamInfo.entrySet().stream().collect(Collectors.toMap(keyUpdater ,valueMapper));
				tumourBaiInfo = tumourBaiInfo.entrySet().stream().collect(Collectors.toMap(keyUpdater, valueMapper));

				String tumourBamMetadataURL = (String) ((List<Map<String,Object>>) tumour.get("available_repos")).get(0).keySet().toArray()[0];
				String tumourGnosID = (String) tumour.get("gnos_id");
				String aliquotID = (String) tumour.get("aliquot_id");
				tumourBamMetadataURL += "cghub/metadata/analysisFull/" + tumourGnosID;
				Map<String, Object> tumourInfo = new TreeMap<String, Object>();
				tumourInfo.put(DATA, tumourBamInfo);
				tumourInfo.put(INDEX, tumourBaiInfo);
				tumourInfo.put(TAG, "tumour");
				tumourInfo.put(NUMBER, i);
				results.put(BAM_TUMOUR_METADATA_URL+"_"+i, tumourBamMetadataURL);
				results.put(TUMOUR_BAM_INFO+"_"+i, tumourInfo);
				results.put(BAM_TUMOUR_GNOS_ID+"_"+i, tumourGnosID);
				results.put(TUMOUR_ALIQUOT_ID+"_"+i, aliquotID);
				String tumourBamRepo =  ((List<String>)tumour.get("gnos_repo")).get(0) + "cghub/data/analysis/download/"+tumourGnosID;
				results.put(TUMOUR_BAM_DOWNLOAD_URL+"_"+i,  tumourBamRepo);
			}
			
			// Get the normal ID from the tumour.
			String aliquotID = (String) (parsedJSON.read("$.normal.aliquot_id", String.class));
			results.put(ALIQUOT_ID, aliquotID);

			//Need to get the VCFs for each tumour aliquot
			int i = 0;
			for (String tumourAliquotID : (List<String>)parsedJSON.read("$.tumors.*.aliquot_id", List.class) )
			{
				// Sanger
				Map<String, Object> sangerInfo = extractFileInfo(jsonFile, jsonPathConfig, "sanger", tumourAliquotID,i);
				results.put(SANGER_VCF_INFO+"_"+i, sangerInfo);

				// DKFZ-EMBL
				Map<String, Object> dkfzemblInfo = extractFileInfo(jsonFile, jsonPathConfig, "dkfz_embl", tumourAliquotID,i);
				results.put(DKFZEMBL_VCF_INFO+"_"+i, dkfzemblInfo);

				// Broad
				Map<String, Object> broadInfo = extractFileInfo(jsonFile, jsonPathConfig, "broad", tumourAliquotID,i);
				results.put(BROAD_VCF_INFO+"_"+i, broadInfo);

				// Muse
				Map<String, Object> museInfo = extractFileInfo(jsonFile, jsonPathConfig, "muse", tumourAliquotID,i);
				results.put(MUSE_VCF_INFO+"_"+i, museInfo);
				i+=1;
			}
			
			// Get OxoQ Score
			String oxoqScore = (String) (parsedJSON.read("$.tumors[0].oxog_score", String.class));
			results.put(OXOQ_SCORE, oxoqScore);

			// Get donor ID
			String submitterDonorID = (String) (parsedJSON.read("$.submitter_donor_id", String.class));
			results.put(SUBMITTER_DONOR_ID, submitterDonorID);

			// Get specimen ID
			String submitterSpecimentID = (String) (parsedJSON.read("$.normal.submitter_specimen_id", String.class));
			results.put(SUBMITTER_SPECIMENT_ID, submitterSpecimentID);

			// Get project code
			String projectCode = (String) (parsedJSON.read("$.project_code", String.class));
			results.put(PROJECT_CODE, projectCode);
			
			// get the repo info
			String sangerRepo = extractRepoInfo(jsonFile, jsonPathConfig, "sanger");
			String broadRepo = extractRepoInfo(jsonFile, jsonPathConfig, "broad");
			String dkfzEmblRepo = extractRepoInfo(jsonFile, jsonPathConfig, "dkfz_embl");
			String museRepo = extractRepoInfo(jsonFile, jsonPathConfig, "muse");
			String normalBamRepo = extractRepoInfo(jsonFile, jsonPathConfig, "normal");
			
			results.put(SANGER_DOWNLOAD_URL, sangerRepo);
			results.put(BROAD_DOWNLOAD_URL, broadRepo);
			results.put(DKFZ_EMBL_DOWNLOAD_URL, dkfzEmblRepo);
			results.put(MUSE_DOWNLOAD_URL, museRepo);
			results.put(NORMAL_BAM_DOWNLOAD_URL, normalBamRepo);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return results;

	}
}
