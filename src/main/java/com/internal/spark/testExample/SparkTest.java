package com.internal.spark.testExample;

import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SparkTest {
	
	public static void main1(String[] args) {
		String inputFile = args[0];
		String outputFile = args[1];
		// Create a Java Spark Context.
		SparkConf conf = new SparkConf().setAppName("wordCount");
		@SuppressWarnings("resource")
		JavaSparkContext sc = new JavaSparkContext(conf);
		// Load our input data.
		JavaRDD<String> input = sc.textFile(inputFile);

		// Split up into words.
		JavaRDD<String> words = input
				.flatMap(new FlatMapFunction<String, String>() {
					private static final long serialVersionUID = 1L;

					public Iterable<String> call(String x) {
						return Arrays.asList(x.split(" "));
					}
				});
		// Transform into word and count.
		JavaPairRDD<String, Integer> counts = words.mapToPair(
				new PairFunction<String, String, Integer>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<String, Integer> call(String x) {
						return new Tuple2<String, Integer>(x, 1);
					}
				}).reduceByKey(new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = 1L;

			public Integer call(Integer x, Integer y) {
				return x + y;
			}
		});
		// Save the word count back out to a text file, causing evaluation.
		counts.saveAsTextFile(outputFile);

	}

	// Word count with InputFormat defined
	public static void main(String[] args) {
		String inputFile = args[0];
		String outputFile = args[1];
		// Create a Java Spark Context.
		SparkConf conf = new SparkConf().setAppName("wordCount");
		@SuppressWarnings("resource")
		JavaSparkContext sparkContext = new JavaSparkContext(conf);

		JavaPairRDD<LongWritable, Text> input = sparkContext.newAPIHadoopFile(inputFile,
				CustomInputFormat.class, LongWritable.class,
				org.apache.hadoop.io.Text.class, sparkContext.hadoopConfiguration());
		// Split up into words.
		JavaRDD<Text> o = input.values();

		JavaRDD<String> words = o.flatMap(new FlatMapFunction<Text, String>() {
			private static final long serialVersionUID = 1L;

			public Iterable<String> call(Text paramT) throws Exception {
				return Arrays.asList(paramT.toString().split(" "));
			}
		});

		// Transform into word and count.
		JavaPairRDD<String, Integer> counts = words.mapToPair(
				new PairFunction<String, String, Integer>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<String, Integer> call(String x) {
						return new Tuple2<String, Integer>(x, 1);
					}
				}).reduceByKey(new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = 1L;

						public Integer call(Integer x, Integer y) {
							return x + y;
						}
				});

		// Save the word count back out to a text file, causing evaluation.
		counts.saveAsTextFile(outputFile);
	}
}
