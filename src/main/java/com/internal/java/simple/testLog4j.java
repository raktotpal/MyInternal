package com.internal.java.simple;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class testLog4j {

  static {
    System.setProperty("className", testLog4j.class.getSimpleName());
    SimpleDateFormat sdf = new SimpleDateFormat("ddMMyyyy-HHmmss");
    System.setProperty("timestamp", sdf.format(new Date()));
  }

  public static void main(String[] args) {
    Logger LOG = Logger.getLogger(testLog4j.class);
    PropertyConfigurator.configure("D:\\log4j.properties");

    LOG.info("I love you xuunn..");

  }

}
