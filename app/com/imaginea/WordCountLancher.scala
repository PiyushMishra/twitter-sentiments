package com.imaginea

import org.apache.spark.launcher.SparkLauncher

object Launcher {

  def runSparkJob(sparkHome: String, appJarPath: String, mainClass: String, sparkMaster: String) {
    val spark = new SparkLauncher().setSparkHome(sparkHome).
      setAppResource(appJarPath).setMainClass(mainClass).
      setDeployMode("cluster").addAppArgs("192.168.2.67", "9200", "/opt/tweetWordCount").
      setMaster(sparkMaster).launch()
    println(scala.io.Source.fromInputStream(spark.getInputStream).getLines().mkString("\n"))
    println(scala.io.Source.fromInputStream(spark.getErrorStream).getLines().mkString("\n"))

    spark.waitFor
  }
}