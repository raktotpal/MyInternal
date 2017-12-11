package com.internal.spark.testExample;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

public class sparkHiveExample {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setMaster("local").setAppName("RPAL-SPARK-SQL");

    SparkContext sc = new SparkContext(conf);
    HiveContext sqlContext = new HiveContext(sc);

    sqlContext.sql("USE rpaltest");
    sqlContext.sql("CREATE TABLE IF NOT EXISTS sparkTableLOAD (id INT, name STRING)"
        + " row format delimited" + " fields terminated by \",\"");
    sqlContext
        .sql("LOAD DATA LOCAL INPATH '/home/bedrock/rpal/firstTest.txt' INTO TABLE sparkTableLOAD");

    // Queries are expressed in HiveQL.
    Row[] results = sqlContext.sql("FROM sparkTableLOAD SELECT id, name").collect();

    for (Row xx : results) {
      System.out
          .println("::::::::::::::::::::::::::::::::+++++++++++++++++++++++++ " + xx.toString());
    }
  }
}