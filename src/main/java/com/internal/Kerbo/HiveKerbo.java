package com.internal.Kerbo;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.security.UserGroupInformation;
/**
 * 
 * Not working 
 * 
 * @author Raktotpal
 *
 */
public class HiveKerbo {
	public static void main(String[] args) throws SQLException, IOException {
		String url = "jdbc:hive2://ec2-52-3-170-100.compute-1.amazonaws.com:10000/avp_b2m;"
				+ "principal=debasmit@ZEBRADEV.COM";
		Connection con = DriverManager.getConnection(url);

		org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
		conf.set("hadoop.security.authentication", "Kerberos");
		UserGroupInformation.setConfiguration(conf);

		UserGroupInformation.loginUserFromKeytab(
				"debasmit@ZEBRADEV.COM",
				"C:\\Users\\Raktotpal\\Downloads\\kerbo\\hive.keytab");
		
		Statement stmt = con.createStatement();
		
		stmt.execute("SHOW DATABASES");

	}
}