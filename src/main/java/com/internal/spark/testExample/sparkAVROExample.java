package com.internal.spark.testExample;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class sparkAVROExample {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName(
				"RPAL-SPARK-SQL");

		SparkContext sc = new SparkContext(conf);

		// sc is an existing JavaSparkContext.
		SQLContext sqlContext = new SQLContext(sc);

		// A JSON dataset is pointed to by path.
		// The path can be either a single text file or a directory storing text
		// files.
		DataFrame people = sqlContext.jsonFile("hdfs://localhost/user/bedrock/rpal/test.json");

		// The inferred schema can be visualized using the printSchema() method.
		people.printSchema();

		people.registerTempTable("rpaltest.people");

	}
}