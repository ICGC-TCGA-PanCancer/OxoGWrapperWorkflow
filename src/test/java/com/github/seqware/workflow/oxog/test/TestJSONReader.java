package com.github.seqware.workflow.oxog.test;

import java.util.Map;

import com.github.seqware.JSONUtils;

public class TestJSONReader {

	

	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Map<String,String> results = JSONUtils.processJSONFile("src/test/resources/testinput.json");
		System.out.println(results);
	}

}
