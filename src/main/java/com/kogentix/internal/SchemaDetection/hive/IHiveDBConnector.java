package com.kogentix.internal.SchemaDetection.hive;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Hashtable;

public interface IHiveDBConnector {
  public boolean createConnection() throws ClassNotFoundException, SQLException;

  public void closeConnection();

  public void execute(String sql) throws SQLException;

  public ResultSet executeQuery(String sql) throws SQLException;

  public void dropTable(String tableName) throws SQLException;

  public void loadJar() throws SQLException;

  public void loadJar(String jarlocation) throws SQLException;

  public void loadUdf(String UDFName, String classname) throws SQLException;

  public void loadVariables(Hashtable<String, String> variables) throws SQLException;

  public void loadUDFs(Hashtable<String, String> udfMap) throws SQLException;
}
