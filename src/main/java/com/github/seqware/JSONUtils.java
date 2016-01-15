package com.github.seqware;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public abstract class JSONUtils {

	static final String OXOQ_SCORE = "OxoQScore";
	static final String BROAD_VCF_OBJECT_ID = "broadVCFObjectID";
	static final String DKFZEMBL_VCF_OBJECT_ID = "dkfzemblVCFObjectID";
	static final String SANGER_VCF_OBJECT_ID = "sangerVCFObjectID";
	static final String BAM_NORMAL_OBJECT_ID = "bamNormalObjectID";
	static final String BAM_TUMOUR_OBJECT_ID = "bamTumourObjectID";
	static final String ALIQUOT_ID = "aliquotID";
	static final String SUBMITTER_DONOR_ID = "submitterDonorID";
	static final String PROJECT_CODE = "projectCode";

	public static Map<String, String> processJSONFile(String filePath) {

		Map<String, String> results = new HashMap<String, String>(8);

		Type simpleMapType = new TypeToken<Map<String, Object>>() {}.getType();
		Gson gson = new Gson();

		Reader json;
		try {
			// TODO: Investigate using JsonPath
			// (https://github.com/jayway/JsonPath) to make this simpler.
			json = new FileReader(filePath);
			Map<String, Object> jsonContents = gson.fromJson(json, simpleMapType);
			
			// Get normal BAM object ID
			Map<String, Object> normal = (Map<String, Object>) jsonContents.get("normal");
			List<Map<String, String>> normalFiles = (List<Map<String, String>>) normal.get("files");
			for (Map<String, String> fileDetails : normalFiles) {
				String fileName = fileDetails.get("file_name");
				if (fileName.endsWith(".bam")) {
					// this.bamNormalObjectID = fileDetails.get("object_id");
					results.put(BAM_NORMAL_OBJECT_ID, fileDetails.get("object_id"));
					// break the for-loop, no need to keep going through other
					// file details.
					break;
				}
			}

			// Get Tumour BAM object ID
			List<Map<String, Object>> tumours = (List<Map<String, Object>>) jsonContents.get("tumors");
			// Eventually, we will have to deal with multi-tumour sitautions
			// properly, but for now lets just assume ONE tumour.
			List<Map<String, String>> tumourFiles = (List<Map<String, String>>) tumours.get(0).get("files");
			for (Map<String, String> fileDetails : tumourFiles) {
				String fileName = fileDetails.get("file_name");
				if (fileName.endsWith(".bam")) {
					// this.bamTumourObjectID = fileDetails.get("object_id");
					results.put(BAM_TUMOUR_OBJECT_ID, fileDetails.get("object_id"));
					// break the for-loop, no need to keep going through other
					// file details.
					break;
				}
			}

			// Get the aliquot ID from the tumour. This may get more complicated
			// in multi-tumour scenarios.
			String aliquotID = (String) tumours.get(0).get("aliquot_id");
			results.put(ALIQUOT_ID, aliquotID);

			// Get VCF Object IDs
			// Sanger
			Map<String, Object> sanger = (Map<String, Object>) jsonContents.get("sanger");
			List<Map<String, String>> sangerFiles = (List<Map<String, String>>) sanger.get("files");
			for (Map<String, String> fileDetails : sangerFiles) {
				String fileName = fileDetails.get("file_name");
				// TODO: We will probably have to download ALL VCFs and then do
				// the merge, like Brian was suggesting, so we'll actually need
				// object IDs for suffixes: ".somatic.snv_mnv.vcf.gz",
				// ".somatic.sv.vcf.gz", ".somatic.indel.vcf.gz"
				if (fileName.endsWith(".somatic.snv_mnv.vcf.gz")) {
					// this.sangerVCFObjectID = fileDetails.get("object_id");
					results.put(SANGER_VCF_OBJECT_ID, fileDetails.get("object_id"));
					// break the for-loop, no need to keep going through other
					// file details.
					break;
				}
			}
			// DFKZ-EMBL
			Map<String, Object> dkfz = (Map<String, Object>) jsonContents.get("dkfz_embl");
			List<Map<String, String>> dkfzFiles = (List<Map<String, String>>) dkfz.get("files");
			for (Map<String, String> fileDetails : dkfzFiles) {
				String fileName = fileDetails.get("file_name");
				// TODO: Get the other VCF files. Also, DKFZ needs to be
				// filtered to not use the "embl-delly" files.
				if (fileName.contains("dkfz-snvCalling") && fileName.endsWith(".somatic.snv_mnv.vcf.gz")) {
					results.put(DKFZEMBL_VCF_OBJECT_ID, fileDetails.get("object_id"));
					// this.dkfzemblVCFObjectID = fileDetails.get("object_id");
					// break the for-loop, no need to keep going through other
					// file details.
					break;
				}
			}
			// Broad
			Map<String, Object> broad = (Map<String, Object>) jsonContents.get("sanger");
			List<Map<String, String>> broadFiles = (List<Map<String, String>>) broad.get("files");
			for (Map<String, String> fileDetails : broadFiles) {
				String fileName = fileDetails.get("file_name");
				// TODO: Broad produces a number of VCFs, need to find out
				// exactly which ones to download.
				if (fileName.contains("broad-mutect") && fileName.endsWith(".somatic.snv_mnv.vcf.gz")) {
					results.put(BROAD_VCF_OBJECT_ID, fileDetails.get("object_id"));
					// this.broadVCFObjectID = fileDetails.get("object_id");
					// break the for-loop, no need to keep going through other
					// file details.
					break;
				}
			}

			// Get OxoQ Score
			String oxoqScore = String.valueOf((Double) jsonContents.get("OxoQ_score"));
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
		}
		return results;

	}
}
