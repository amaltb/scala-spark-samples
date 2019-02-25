package com.ab.examples.spark_samples

import org.apache.spark.sql.{Dataset, SparkSession}

object DataSetSimpleApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Simple Application").master("local").getOrCreate()
    val logFile = "/Users/ambabu/Documents/Applications/spark-2.3.3-bin-without-hadoop/README.md"

    val logData: Dataset[String] = spark.read.textFile(logFile).cache()

    val numAs = logData.filter(_.contains("a")).count()
    val numBs = logData.filter(_.contains("b")).count()

    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }

}
