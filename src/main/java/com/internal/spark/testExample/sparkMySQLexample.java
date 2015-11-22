package com.internal.spark.testExample;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.JdbcRDD;

import scala.reflect.ClassManifestFactory;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;

/**
 * Run as "./spark-submit --jars <external jar> --class <Main-Class> --master <mode> <Application-jar>"
 * @author Rpal
 *
 */
public class sparkMySQLexample {

	public static void main(String[] args) throws ClassNotFoundException {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("RPAL-SPARK-SQL");
		@SuppressWarnings("resource")
		JavaSparkContext context = new JavaSparkContext(conf);
		
		Class.forName("com.mysql.jdbc.Driver");
		
		DbConnection connection = new DbConnection(
				"com.mysql.jdbc.Driver",
				"jdbc:mysql://localhost:3306/hive", "hive", "hive");
		JdbcRDD<Object[]> jdbcRDD = new JdbcRDD<Object[]>(context.sc(),
				connection, "select * from hive.VERSION where VER_ID >= ? and VER_ID <= ?", 1, 1000, 1,
				new MapResult(), ClassManifestFactory.fromClass(Object[].class));

		JavaRDD<Object[]> javaRDD = JavaRDD.fromRDD(jdbcRDD,
				ClassManifestFactory.fromClass(Object[].class));

		List<String> employeeFullNameList = javaRDD.map(new Function<Object[], String>() {
			private static final long serialVersionUID = 1L;

			public String call(final Object[] record) throws Exception {
		        return record[0].toString();
		    }
		}).collect();
		 
		for (String fullName : employeeFullNameList) {
		    System.out.println("::::::::::: " + fullName);
		}
	}
}

class DbConnection extends AbstractFunction0<Connection> implements
		Serializable {
	private static final long serialVersionUID = 1L;
	private String driverClassName;
	private String connectionUrl;
	private String userName;
	private String password;

	public DbConnection(String driverClassName, String connectionUrl,
			String userName, String password) {
		this.driverClassName = driverClassName;
		this.connectionUrl = connectionUrl;
		this.userName = userName;
		this.password = password;
	}

	public Connection apply() {
		try {
			Class.forName(driverClassName);
		} catch (ClassNotFoundException e) {
			System.out.println("Failed to load driver class:: "
					+ e.getMessage());
		}

		Properties properties = new Properties();
		properties.setProperty("user", userName);
		properties.setProperty("password", password);

		Connection connection = null;
		try {
			connection = DriverManager.getConnection(connectionUrl, properties);
		} catch (SQLException e) {
			System.out.println("Connection failed:: " + e.getMessage());
		}

		return connection;
	}
}

class MapResult extends AbstractFunction1<ResultSet, Object[]> implements
		Serializable {
	private static final long serialVersionUID = 1L;

	public Object[] apply(ResultSet row) {
		return JdbcRDD.resultSetToObjectArray(row);
	}
}