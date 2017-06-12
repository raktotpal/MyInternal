package com.kogentix.internal.SchemaDetection.hive;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

public class HiveDBConnector implements IHiveDBConnector {
  public static final Logger LOG = Logger.getLogger(HiveDBConnector.class);
  private String driverName = null;
  private Connection conn = null;
  private String userName;
  private String password;
  private Statement stmt = null;
  private String connectionString;
  private String url = null;
  private String jarLocation;

  private String ip;
  private int port;
  private String dbName;

  private static final String RESOURCE_VM_ARG = "resource_loc";
  private static PropertiesConfiguration propConfig;

  /**
   * LOAD PROPERTIES
   */
  static {
    /*
     * static block to set the properties 'className' and 'timestamp' which are
     * picked from log4j property to name the log files.
     */
    try {
      String fileLocation = System.getProperty(RESOURCE_VM_ARG);
      File file = new File(fileLocation, "config.properties");
      LOG.info("Config file location: " + file.getAbsolutePath());

      System.out.println("Config file location: " + file.getAbsolutePath());

      propConfig = new PropertiesConfiguration(file);
    } catch (ConfigurationException ConfEx) {
      LOG.error("Configuration Exception: " + ConfEx.getMessage());
    }
  }

  public HiveDBConnector() {
    this.driverName = propConfig.getString(ApplicationConstants.HIVE_JDBCDRIVER);
    this.userName = propConfig.getString(ApplicationConstants.HIVE_USERNAME).toString().trim();
    this.password = propConfig.getString(ApplicationConstants.HIVE_PASSWORD).toString().trim();
    this.url = propConfig.getString(ApplicationConstants.HIVE_CONNECTIONURL).trim();

    this.ip = propConfig.getString(ApplicationConstants.HIVE_IP).toString().trim();
    this.port = Integer.parseInt(propConfig.getString(ApplicationConstants.HIVE_PORT).trim());
    this.dbName = propConfig.getString(ApplicationConstants.HIVE_DBNAME).toString().trim();

    LOG.info("**************************************************");

    LOG.info(driverName + " :: " + userName + " :: " + password + " :: " + url + " :: " + ip
        + " :: " + port + " :: " + dbName);

    LOG.info("**************************************************");
  }

  /**
   * This method is responsible for creating hive server connection
   * 
   * @return connection object of hive server
   * @throws ClassNotFoundException
   * @throws SQLException
   */
  public boolean createConnection() throws ClassNotFoundException, SQLException {
    Class.forName(driverName);
    String hiveMode = propConfig.getString(ApplicationConstants.HIVE_MODE);
    if (hiveMode.equalsIgnoreCase("E")) {
      if (conn == null) {
        connectionString = url;
        conn = DriverManager.getConnection(connectionString, userName, password);
        stmt = conn.createStatement();
        return true;
      }
    } else {
      if (conn == null) {
        connectionString = url + ip + ":" + port + "/" + dbName;
        LOG.info("Hive-Credentials:: " + connectionString + " : " + userName + " : " + password);
        conn = DriverManager.getConnection(connectionString, userName, password);
        stmt = conn.createStatement();
        return true;
      }
    }
    return false;
  }

  public void closeConnection() {
    try {
      if (null != stmt) {
        stmt.close();
      }
      if (null != conn) {
        conn.close();
      }
    } catch (SQLException e) {}
  }

  /**
   * Executes hiveql queries
   * 
   * @param sql
   * @throws SQLException
   */
  public void execute(String sql) throws SQLException {
    stmt.execute(sql);
  }

  /**
   * Executes hiveql queries
   * 
   * @param sql
   * @throws SQLException
   */
  public ResultSet executeQuery(String sql) throws SQLException {
    return stmt.executeQuery(sql);
  }

  /**
   * Drops a given table
   * 
   * @param tableName
   * @throws SQLException
   */
  public void dropTable(String tableName) throws SQLException {
    String tmp = "DROP TABLE " + tableName;
    stmt.execute(tmp);
  }

  /**
   * Adds external jars to hive from a location specified in the properties file
   * 
   * @throws SQLException
   */
  public void loadJar() throws SQLException {
    String tmp = "ADD JAR " + jarLocation;
    LOG.debug(tmp);
    execute(tmp);
  }

  /**
   * Adds external jars to hive from the given location
   * 
   * @param jarlocation
   * @throws SQLException
   */
  public void loadJar(String jarStatement) throws SQLException {
    String tmp = jarStatement;
    LOG.debug(tmp);
    execute(tmp);
  }

  /**
   * Creates a temporary function using a class present in a jar, which has been
   * already loaded.
   * 
   * @param UDFName
   * @param classname
   *          Fully qualified class name where UDF resides
   * @throws SQLException
   */
  public void loadUdf(String UDFName, String classname) throws SQLException {
    String sql = "CREATE TEMPORARY FUNCTION " + UDFName + " AS '" + classname + "'";
    LOG.debug(sql);
    execute(sql);
  }

  /**
   * It loads user defined variables into hive session
   * 
   * @param variables
   * @throws SQLException
   */
  public void loadVariables(Hashtable<String, String> variables) throws SQLException {
    Iterator<Entry<String, String>> iterator = variables.entrySet().iterator();
    String query = null;
    while (iterator.hasNext()) {
      Entry<String, String> entry = iterator.next();
      query = "SET" + " " + entry.getKey().toLowerCase() + "=" + entry.getValue();
      LOG.debug("setting variable into hive session" + " " + conn.hashCode() + " " + query);
      execute(query);
    }
  }

  public void loadUDFs(Hashtable<String, String> udfMap) throws SQLException {
    Set<String> udfs = udfMap.keySet();
    for (String key : udfs) {
      loadUdf(key, udfMap.get(key));
    }
  }
}