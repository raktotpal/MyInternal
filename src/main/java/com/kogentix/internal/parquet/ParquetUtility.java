package com.kogentix.internal.parquet;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import parquet.Preconditions;
import parquet.example.data.Group;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.example.GroupReadSupport;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

public class ParquetUtility {

  private static final Logger LOG = Logger.getLogger(ParquetUtility.class);

  public static final String CSV_DELIMITER = ",";

  private static String readFile(String path) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(path));
    StringBuilder stringBuilder = new StringBuilder();

    try {
      String line = null;
      String ls = System.getProperty("line.separator");

      while ((line = reader.readLine()) != null) {
        stringBuilder.append(line);
        stringBuilder.append(ls);
      }
    } finally {
      closeQuietly(reader);
    }

    return stringBuilder.toString();
  }

  public static String getSchema(File csvFile) throws IOException {
    String fileName = csvFile.getName().substring(0, csvFile.getName().length() - ".csv".length())
        + ".schema";
    File schemaFile = new File(csvFile.getParentFile(), fileName);
    return readFile(schemaFile.getAbsolutePath());
  }

  public static void convertCsvToParquet(Map<String, String> schemaInfo, File csvFile,
      File outputParquetFile) throws IOException {
    convertCsvToParquet(schemaInfo, csvFile, outputParquetFile, false);
  }

  public static void convertCsvToParquet(Map<String, String> schemaInfo, File csvFile,
      File outputParquetFile, boolean enableDictionary) throws IOException {
    LOG.info("Converting " + csvFile.getName() + " to " + outputParquetFile.getName());

    String rawSchema = prepareSchema(schemaInfo);

    // "message m { optional int64 id; "
    // + "optional binary name; "
    // + "optional binary address;}";
    // getSchema(csvFile);

    if (outputParquetFile.exists()) {
      throw new IOException(
          "Output file " + outputParquetFile.getAbsolutePath() + " already exists");
    }

    Path path = new Path(outputParquetFile.toURI());

    MessageType schema = MessageTypeParser.parseMessageType(rawSchema);
    CsvParquetWriter writer = new CsvParquetWriter(path, schema, enableDictionary);

    BufferedReader br = new BufferedReader(new FileReader(csvFile));
    String line;
    int lineNumber = 0;
    try {
      while ((line = br.readLine()) != null) {
        String[] fields = line.split(Pattern.quote(CSV_DELIMITER));
        writer.write(Arrays.asList(fields));
        ++lineNumber;
      }
      writer.close();
    } finally {
      LOG.info("Number of lines: " + lineNumber);
      closeQuietly(br);
    }
  }

  private static String prepareSchema(Map<String, String> schemaInfo) {
    // TODO Auto-generated method stub
    return null;
  }

  public static void convertParquetToCSV(File parquetFile, File csvOutputFile) throws IOException {
    Preconditions.checkArgument(parquetFile.getName().endsWith(".parquet"),
        "parquet file should have .parquet extension");
    Preconditions.checkArgument(csvOutputFile.getName().endsWith(".csv"),
        "csv file should have .csv extension");
    Preconditions.checkArgument(!csvOutputFile.exists(),
        "Output file " + csvOutputFile.getAbsolutePath() + " already exists");

    LOG.info("Converting " + parquetFile.getName() + " to " + csvOutputFile.getName());

    Path parquetFilePath = new Path(parquetFile.toURI());

    Configuration configuration = new Configuration(true);

    GroupReadSupport readSupport = new GroupReadSupport();
    ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, parquetFilePath);
    MessageType schema = readFooter.getFileMetaData().getSchema();

    readSupport.init(configuration, null, schema);
    BufferedWriter w = new BufferedWriter(new FileWriter(csvOutputFile));
    ParquetReader<Group> reader = new ParquetReader<Group>(parquetFilePath, readSupport);
    try {
      Group g = null;
      while ((g = reader.read()) != null) {
        writeGroup(w, g, schema);
      }
      reader.close();
    } finally {
      closeQuietly(w);
    }
  }

  private static void writeGroup(BufferedWriter w, Group g, MessageType schema) throws IOException {
    for (int j = 0; j < schema.getFieldCount(); j++) {
      if (j > 0) {
        w.write(CSV_DELIMITER);
      }
      String valueToString = g.getValueToString(j, 0);
      w.write(valueToString);
    }
    w.write('\n');
  }

  private static void closeQuietly(Closeable res) {
    try {
      if (res != null) {
        res.close();
      }
    } catch (IOException ioe) {
      LOG.warn("Exception closing reader " + res + ": " + ioe.getMessage());
    }
  }
}