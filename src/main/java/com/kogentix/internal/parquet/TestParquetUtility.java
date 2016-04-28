package com.kogentix.internal.parquet;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TestParquetUtility {
	public static void main(String[] args) throws IOException {

		String schemaInString = "id,name,address";

		Map<String, String> schemaAsMap = new HashMap<String, String>();

		ParquetUtility
				.convertCsvToParquet(
						schemaAsMap,
						new File(
								"C:\\Users\\RAKTOTPAL\\Desktop\\myTestToParquet.csv"),
						new File(
								"C:\\Users\\RAKTOTPAL\\Desktop\\outputParquet.parquet"));
	}
}