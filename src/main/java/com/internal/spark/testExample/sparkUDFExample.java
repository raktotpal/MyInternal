package com.internal.spark.testExample;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

public class sparkUDFExample {
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

		people.registerTempTable("people");
		
		udf1Test(sqlContext, "name", "people");
		
		

	}
	
	private static void udf1Test(SQLContext sqlContext, String colName, String tableName) {
		// With Java 8 lambdas:
		// sqlContext.registerFunction(
		// "stringLengthTest", (String str) -> str.length(), DataType.IntegerType);
		 
		sqlContext.udf().register("stringLengthTest",
				new UDF1<String, Integer>() {
					public Integer call(String str) throws Exception {
						return str.length();
					}
				}, DataTypes.IntegerType);

		DataFrame result = sqlContext.sql("SELECT stringLengthTest("
				+ colName + ") FROM " + tableName);
		
		System.out.println(":::::::::::::: " + result.collect()[0].get(0));
	}
		
	@SuppressWarnings("unused")
	private void udf2Test(SQLContext sqlContext) {
		// With Java 8 lambdas:
		// sqlContext.registerFunction(
		// "stringLengthTest",
		// (String str1, String str2) -> str1.length() + str2.length,
		// DataType.IntegerType);
		 
		sqlContext.udf().register("stringLengthTest",
				new UDF2<String, String, Integer>() {
					public Integer call(String str1, String str2)
							throws Exception {
						return str1.length() + str2.length();
					}
				}, DataTypes.IntegerType);

		Row result = sqlContext.sql("SELECT stringLengthTest('test', 'test2')")
				.head();
	}
}