package com.internal.spark.testExample;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;


/**
 * Read csv with pojo and load that to table.
 * save the table as JSON
 * 
 * @author rbordoloi
 *
 */
public class sparkSQLCSVExample {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName(
				"RPAL-SPARK-SQL-CSV");
		
		JavaSparkContext ctx = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(ctx);
		
		// Load a text file and convert each line to a JavaBean.
		JavaRDD<Person> people = ctx.textFile("/home/bedrock/rpal/people.txt").map(
		  new Function<String, Person>() {
			private static final long serialVersionUID = -9190778346995865364L;

			public Person call(String line) throws Exception {
		      String[] parts = line.split(",");

		      Person person = new Person();
		      person.setName(parts[0]);
		      person.setAge(Integer.parseInt(parts[1].trim()));

		      return person;
		    }
		  });
		
		// Apply a schema to an RDD of JavaBeans and register it as a table.
		DataFrame schemaPeople = sqlContext.createDataFrame(people, Person.class);
		schemaPeople.registerTempTable("people");
		
		schemaPeople.toJSON().saveAsTextFile("/home/bedrock/rpal/answer.json");;
		
	}
}