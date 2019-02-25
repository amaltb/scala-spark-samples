package com.ab.examples.spark_samples.utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object SparkUtils {
  /**
    * Creating an RDD[String] from a textfile. This is a lazy evaluation and
    * RDD[String] would actually be created only at the time of first action call. So, there is not point in checking for
    * InvalidInputException here, as there is not RDD action happening inside this method...
   */
  def createRDDFromTextFile(sc: SparkContext, filePath: String, local: Boolean = false): RDD[String] =
  {
    val inputPath = if (local) "file://" + filePath else filePath
    sc.textFile(inputPath).cache()
  }

  def getOrCreateSparkSession(): SparkSession = SparkSession.builder().getOrCreate()

  def getOrCreateSparkSession(appName: String, master: String): SparkSession = SparkSession.builder()
    .appName(appName)
    .master(master)
    .getOrCreate()

  def closeSession(spark: SparkSession): Unit = spark.close()
}
