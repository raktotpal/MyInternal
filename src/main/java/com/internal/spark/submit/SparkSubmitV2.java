package com.internal.spark.submit;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

public class SparkSubmitV2 {

  public static void main(String[] args) throws IOException, InterruptedException {
    System.out.println("Hi HELLO WORLD");

    String sparkHome = "/opt/cloudera/parcels/CDH/lib/spark";
    String yarnConfDir = "/etc/hadoop/conf";
    String sparkMainClass = "com.neilsen.pdm.main.StartEngine";
    String sparkMaster = "yarn"; // yarn
    String sparkDeployMode = "client";
    String sparkJarPath = "/home/transengine/rpal/test02/mainTestSparkSubmit.jar";

    Map<String, String> env = new HashMap<String, String>();
    env.put("HADOOP_CONF_DIR", yarnConfDir);
    env.put("YARN_CONF_DIR", yarnConfDir);

    SparkLauncher launcher = new SparkLauncher(env).setSparkHome(sparkHome)
        .setAppResource(sparkJarPath).setMainClass(sparkMainClass).setMaster(sparkMaster)
        .setDeployMode(sparkDeployMode).setVerbose(true);

    // .setConf("spark.app.id", "AppID if you have one")
    // .setConf("spark.driver.memory", "8g")
    // .setConf("spark.akka.frameSize", "200")
    // .setConf("spark.executor.memory", "2g")
    // .setConf("spark.executor.instances", "32")
    // .setConf("spark.executor.cores", "32")
    // .setConf("spark.default.parallelism", "100")
    // .setConf("spark.driver.allowMultipleContexts","true")

    SparkAppHandle sparkAppHandle = launcher.startApplication();

    System.out.println(sparkAppHandle.getAppId());
    System.out.println(sparkAppHandle.getState());

    // Process sparkProcess = launcher.launch();
    //
    // InputStreamReaderRunnable inputStreamReaderRunnable = new
    // InputStreamReaderRunnable(sparkProcess.getInputStream(), "input");
    // Thread inputThread = new Thread(inputStreamReaderRunnable,
    // "LogStreamReader input");
    // inputThread.start();
    //
    // InputStreamReaderRunnable errorStreamReaderRunnable = new
    // InputStreamReaderRunnable(sparkProcess.getErrorStream(), "error");
    // Thread errorThread = new Thread(errorStreamReaderRunnable,
    // "LogStreamReader error");
    // errorThread.start();
    //
    // System.out.println("Waiting for finish...");
    // int exitCode = sparkProcess.waitFor();
    // System.out.println("Finished! Exit code:" + exitCode);

  }

}
