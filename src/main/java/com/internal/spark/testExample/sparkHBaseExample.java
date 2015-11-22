package com.internal.spark.testExample;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class sparkHBaseExample {
	public static void main(String[] args) throws IOException {
		SparkConf spconf = new SparkConf().setMaster("local").setAppName(
				"RPAL-SPARK-SQL-HBase");

		List<String> jars = new ArrayList<String>();

		// spconf.setSparkHome("/opt/human/opt/spark-0.9.0-hdp1");
		spconf.setJars(jars.toArray(new String[jars.size()]));
		JavaSparkContext sc = new JavaSparkContext(spconf);
		// spconf.set("spark.executor.memory", "1g");

		SQLContext jsql = new SQLContext(sc);

		HBaseConfiguration conf = new HBaseConfiguration();
		String tableName = "HBase.CounData1_Raw_Min1";
		HTable table = new HTable(conf, tableName);
		try {

			ResultScanner scanner = table.getScanner(new Scan());
			List<String> jsonList = new ArrayList<String>();

			String json = null;

			for (Result rowResult : scanner) {
				json = "";
				String rowKey = Bytes.toString(rowResult.getRow());
				for (byte[] s1 : rowResult.getMap().keySet()) {
					String s1_str = Bytes.toString(s1);

					String jsonSame = "";
					for (byte[] s2 : rowResult.getMap().get(s1).keySet()) {
						String s2_str = Bytes.toString(s2);
						for (long s3 : rowResult.getMap().get(s1).get(s2)
								.keySet()) {
							String s3_str = new String(rowResult.getMap()
									.get(s1).get(s2).get(s3));
							jsonSame += "\"" + s2_str + "\":" + s3_str + ",";
						}
					}
					jsonSame = jsonSame.substring(0, jsonSame.length() - 1);
					json += "\"" + s1_str + "\"" + ":{" + jsonSame + "}" + ",";
				}
				json = json.substring(0, json.length() - 1);
				json = "{\"RowKey\":\"" + rowKey + "\"," + json + "}";
				jsonList.add(json);
			}

			JavaRDD<String> jsonRDD = sc.parallelize(jsonList);

			DataFrame schemaRDD = jsql.jsonRDD(jsonRDD);

			System.out.println(schemaRDD.take(2));

		} finally {
			table.close();
		}

	}
}