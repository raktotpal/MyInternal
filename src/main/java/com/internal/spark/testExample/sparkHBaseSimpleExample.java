package com.internal.spark.testExample;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class sparkHBaseSimpleExample {
	public static void main(String[] args) throws IOException {
		SparkConf spconf = new SparkConf().setMaster("local").setAppName(
				"RPAL-SPARK-SQL-HBase");

//		List<String> jars = new ArrayList<String>();
//		spconf.setJars(jars.toArray(new String[jars.size()]));

		JavaSparkContext jsc = new JavaSparkContext(spconf);

		spconf.set(TableInputFormat.INPUT_TABLE, "tablename");
		JavaPairRDD<ImmutableBytesWritable, Result> data = jsc.newAPIHadoopRDD(
				jsc.hadoopConfiguration(), TableInputFormat.class,
				ImmutableBytesWritable.class, Result.class);

		System.out.println("*************** No. of records found ---- " + data.count());
	}
}