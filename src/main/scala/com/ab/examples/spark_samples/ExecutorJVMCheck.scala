package com.ab.examples.spark_samples
import java.lang.management.ManagementFactory

import org.apache.spark.sql.SparkSession

object ExecutorJVMCheck {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("ExecutorJVMCheck").getOrCreate()

    val sc = session.sparkContext

    val data = Seq(1, 2, 3, 4, 5, 6, 7, 8)

    val seqRDD = sc.parallelize(data, 4)

    println(seqRDD.map(x => {
      val jvmID = ManagementFactory.getRuntimeMXBean.getName
      x + " -> " + jvmID
    }).collect().toList)

    sc.stop()
  }
}
