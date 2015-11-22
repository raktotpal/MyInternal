package com.kogentix.internal.jsonStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class ParseJSON {
	public static void main(String[] args) throws JSONException, IOException {

		// String str =
		// "[{\"name\":\"name1\",\"url\":\"url1\"},{\"name\":\"name2\",\"url\":\"url2\"}]";

		FileInputStream fisTargetFile = new FileInputStream(new File(
				"C:\\Users\\Raktotpal\\Desktop\\testJSON.txt"));

		String targetFileStr = IOUtils.toString(fisTargetFile, "UTF-8");

		JSONObject jsonObj = new JSONObject(targetFileStr);

		System.out.println(jsonObj.toString());
		
		Iterator it = jsonObj.keys();
		
		JSONObject myJ = new JSONObject();
		
		if (it.hasNext()) {
			String key = (String) it.next();
			
			if (jsonObj.get(key) instanceof JSONArray) {
				
			} else {
				myJ.putOpt(key, jsonObj.get(key).toString());
			}
		}
		
		System.out.println("=================>> " + myJ.toString());
		
	}
}