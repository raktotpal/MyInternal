package com.internal.spark.stockExercise;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class Stocks {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName(
				"RPAL-SPARK-SQL");

		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// SparkContext sc = new SparkContext(conf);
		// HiveContext hiveContext = new HiveContext(sc);
		
		SQLContext sqlContext = new SQLContext(sc);

		

		JavaRDD<String> people = sc
				.textFile("hdfs://localhost/user/bedrock/rpal/stocks/");

		
		// The schema is encoded in a string
		String schemaString = people.take(1).get(0);
		
		System.out.println("::::::::::::::::::::: " + schemaString);

		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<StructField>();
		for (String fieldName: schemaString.split(",")) {
		  fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
		  System.out.println(":::::::::::::::::::::>>>>>>>>>>> " + fieldName);
		}
		StructType schema = DataTypes.createStructType(fields);
		System.out.println("**************************************************************");

		// Convert records of the RDD (people) to Rows.
		JavaRDD<Row> rowRDD = people.map(
		  new Function<String, Row>() {
			private static final long serialVersionUID = 1L;

			public Row call(String record) throws Exception {
		      Object[] fields_obj = record.split(",");
		      return RowFactory.create(fields_obj);
		    }
		  });

		// Apply the schema to the RDD.
		DataFrame peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema);

		// Register the DataFrame as a table.
		peopleDataFrame.registerTempTable("people");
		
		System.out.println("#########################################################");
		peopleDataFrame.printSchema();
		
		// SQL can be run over RDDs that have been registered as tables.
		DataFrame results = sqlContext.sql("SELECT * FROM people");
		
		results.show();
	}
}