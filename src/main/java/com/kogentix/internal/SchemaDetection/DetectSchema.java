package com.kogentix.internal.SchemaDetection;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.kogentix.internal.SchemaDetection.hive.HiveDBConnector;

public class DetectSchema {
	private static final Logger LOG = Logger.getLogger(DetectSchema.class);
	static BufferedReader br;

	static HiveDBConnector hiveConnector = new HiveDBConnector();
	static HiveDBConnector hiveConnector1 = new HiveDBConnector();
	static HiveDBConnector hiveConnector2 = new HiveDBConnector();

	public static void main(String[] args) throws JSONException,
			FileNotFoundException, ClassNotFoundException, SQLException {
		
		PropertyConfigurator.configure(System.getProperty("log4j.configuration"));
		
		//String fileLoc = "C:\\Users\\Raktotpal\\Desktop\\sparkTest.txt";
		String fileLoc = "/home/raktotpal/Desktop/sparkTest.txt";
		String fileLocInHDFS = "/rpal/data1";
		
		String hiveDBname = "myTestHiveTable";
		String hiveTableName = "myTestHiveTable.myTable";
		
		String lookUpTableInHive = "lookUp.lookUpTable";

		JSONObject jsonObj = new JSONObject();
		JSONArray jsonArray = new JSONArray();

		String dataRow = StringUtils.EMPTY;

		br = new BufferedReader(new FileReader(fileLoc));

		try {
			if ((dataRow = br.readLine()) != null) {
				System.out.println(dataRow);
			}
		} catch (IOException ioEx) {
			ioEx.printStackTrace();
		}

		String[] data = dataRow.toString().split(",");
		
		StringBuffer sbForSchema = new StringBuffer("CREATE EXTERNAL TABLE " + hiveTableName + " (");

		SimpleDateFormat dateFmt = new SimpleDateFormat("mm/dd/yyyy");

		if (dataRow != null) {
			for (int x = 0; x < data.length; x++) {
				String d = data[x];

				JSONObject tempJSONObj = new JSONObject();

				try {
					dateFmt.parse(d);
					tempJSONObj.putOpt("Column_" + String.valueOf(x), "Date");
					
					sbForSchema.append("Column_" + String.valueOf(x) + " Date,");

				} catch (ParseException e1) {
					try {
						Integer.parseInt(d);
						tempJSONObj.putOpt("Column_" + String.valueOf(x), "int");
						
						sbForSchema.append("Column_" + String.valueOf(x) + " int,");

					} catch (NumberFormatException e) {
						try {
							Float.parseFloat(d);
							tempJSONObj.putOpt("Column_" + String.valueOf(x), "float");
							
							sbForSchema.append("Column_" + String.valueOf(x) + " float,");

						} catch (NumberFormatException ex) {
							try {
								Double.parseDouble(d);
								tempJSONObj.putOpt("Column_" + String.valueOf(x), "double");
								
								sbForSchema.append("Column_" + String.valueOf(x) + " double,");

							} catch (NumberFormatException nfe) {

								tempJSONObj.putOpt("Column_" + String.valueOf(x), "String");
								
								sbForSchema.append("Column_" + String.valueOf(x) + " String,");
							}
						}
					}
				}
				jsonArray.put(tempJSONObj);
			}
			jsonObj.put("schema", jsonArray);
			
			sbForSchema.deleteCharAt(sbForSchema.length() - 1);
		}
		LOG.info("----->> " + jsonObj.toString());

		sbForSchema.append(") ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '" + fileLocInHDFS + "'");
		
		try {
			hiveConnector.createConnection();
			
			hiveConnector.execute("CREATE DATABASE IF NOT EXISTS " + hiveDBname);
			hiveConnector.execute("USE " + hiveDBname);
			
			hiveConnector.execute(sbForSchema.toString());
			
			ResultSet testResult = hiveConnector.executeQuery("DESCRIBE " + hiveTableName);
			
			while (testResult.next()) {
				
				Map<String, Integer> scoreCard = new HashMap<String, Integer>();

				System.out.println("________________________________________>>> " + testResult.getString(1) + " oooooooooooooooo "
						+ testResult.getString(2));
				
				hiveConnector1.createConnection();
				ResultSet rs = hiveConnector1.executeQuery("SELECT " + testResult.getString(1) + " FROM " + hiveTableName);
				
				while (rs.next()) {
					String columnENtry = rs.getString(1);
					
					hiveConnector2.createConnection();
					ResultSet lookupSet = hiveConnector2
							.executeQuery("SELECT predictedcolumn, score FROM "
									+ lookUpTableInHive + " WHERE columnentry = '" + columnENtry + "' AND datatype = '" + testResult.getString(2) + "'");
					
					
					if (lookupSet.next()) {
						if (scoreCard.containsKey(lookupSet.getString(1))) {
							scoreCard.put(lookupSet.getString(1),
									scoreCard.get(lookupSet.getString(1)) + lookupSet.getInt(2));
						} else {
							scoreCard.put(lookupSet.getString(1), lookupSet.getInt(2));
						}

					}
					

				}
				
				Iterator<?> it = scoreCard.entrySet().iterator();
				
				while (it.hasNext()) {
					Map.Entry pair = (Map.Entry)it.next();
			        
					System.out.println(pair.getKey() + " =<>==<>==<>==<>==<>==<>==<>==<>==<>==<>= " + pair.getValue());
				}
				
			}
			
		} catch (ClassNotFoundException cnfEx) {
			LOG.error("ClassNotFoundException: " + cnfEx.getMessage());
			throw new ClassNotFoundException();
		} catch (SQLException sqlEx) {
			LOG.error("SQLException: " + sqlEx.getMessage());
			throw new SQLException();
		} finally {
			hiveConnector.closeConnection();
			hiveConnector1.closeConnection();
			hiveConnector2.closeConnection();
		}

	}
}