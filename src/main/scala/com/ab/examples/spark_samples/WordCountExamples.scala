package com.ab.examples.spark_samples

import com.ab.examples.spark_samples.utils.SparkUtils
import org.apache.hadoop.mapred.InvalidInputException
import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object WordCountExamples {

  private var spark: SparkSession = _
  private var sc: SparkContext = _

  def main(args: Array[String]): Unit = {

    val log = LogManager.getLogger("myLogger")
    log.info("WordCountExamples spark application begins...")

    spark = SparkUtils.getOrCreateSparkSession("WordCountExamples", "local")
    sc = spark.sparkContext

    val fileRdd = SparkUtils.createRDDFromTextFile(sc,
      "/Applications/Brackets.app/Contents/samples/root/Getting Started/Office/Work_Queries")

    val wordPairRdd = WordCountMapper.createWordPairRdd(fileRdd, " ")

    // word count using reduceByKey
    try {
      val wordCountRddRBK = wordPairRdd
        .reduceByKey((a, b) => a + b)


      println(wordCountRddRBK.collect().toList)
    }
    catch {
      case e1: InvalidInputException => println("Input path specified is invalid.. \nDetails: " + e1)
      case unknown: Exception => println("Unknown exception... \nDetails: " + unknown)
    }

    // word count using groupByKey (not preferred due to the lack of partial grouping...
    // might explode the reducer memory with lot of entries)
    try {
      val wordCountRddGBK = wordPairRdd
        .groupByKey(4)
        .map(x => (x._1, x._2.toList.size))


      println(wordCountRddGBK.collect().toList)
    }
    catch {
      case e1: InvalidInputException => println("In GBK Input path specified is invalid.. \nDetails: " + e1)
      case unknown: Exception => println("Unknown exception... \nDetails: " + unknown)
    }


    // wordcount using aggregateByKey
    try {
      val wordCountRddABK = wordPairRdd
        .aggregateByKey(0)((acc, b) => acc + 1, (a, b) => a + b)


      println(wordCountRddABK.collect().toList)
    }
    catch {
      case e1: InvalidInputException => println("In GBK Input path specified is invalid.. \nDetails: " + e1)
      case unknown: Exception => println("Unknown exception... \nDetails: " + unknown)
    }


    // wordcount using combineByKey... (initial value can be a function applicable on the iterable)
    try {
      val wordCountRddCBK = wordPairRdd
        .combineByKey((a:Int) => a, (acc: Int, v: Int) => acc + v, (acc1: Int, acc2: Int) => acc1 + acc2)


      println(wordCountRddCBK.collect().toList)
    }
    catch {
      case e1: InvalidInputException => println("In GBK Input path specified is invalid.. \nDetails: " + e1)
      case unknown: Exception => println("Unknown exception... \nDetails: " + unknown)
    }

    SparkUtils.closeSession(spark)
  }
}

object WordCountMapper extends Serializable {

  // making log object transient, as it is not serializable, and hence can not be part of the closure
  @transient
  lazy val log = LogManager.getLogger("MyLogger")

  def createWordPairRdd(fileRdd: RDD[String], delimiter: String): RDD[(String, Int)] = {
    log.info("Converting the lines to word pair by splitting at: " + delimiter + "...")
    fileRdd.flatMap(_.split(" ")).map(x => (x, 1))
  }
}
