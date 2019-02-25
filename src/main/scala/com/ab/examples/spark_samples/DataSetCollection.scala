package com.ab.examples.spark_samples

import com.ab.examples.spark_samples.utils.SparkUtils
import org.apache.hadoop.mapred.InvalidInputException
import org.apache.log4j.LogManager
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

case class Person(name: String, age: Long)
case class Student(name: String, marks: String)
case class Student1(id: Int, subject: String, mark: Double)

object DataSetCollection {
  private val log = LogManager.getLogger("myLogger")

  def dsFromRdd(spark: SparkSession): Unit = {
    import spark.implicits._

    try {
      val personDS = spark.sparkContext
        .textFile("/Users/ambabu/Documents/Applications/spark-2.3.3-bin-without-hadoop/examples/src/main/resources/people.txt")
        .map(_.split(","))
        .map(arr => Person(arr(0), arr(1).toInt))
        .toDS()

      println("\n\nSchema: ")
      personDS.printSchema()
    }
    catch {
      case e: InvalidInputException => log.error("Invalid input path... \nDetails: " + e)
    }
    finally {
      SparkUtils.closeSession(spark)
    }
  }

  def dsProgrammatically(spark: SparkSession): Unit = {
    try {
      val fileRdd = spark.sparkContext
        .textFile("/Users/ambabu/Documents/Applications/spark-2.3.3-bin-without-hadoop/examples/src/main/resources/people.txt")
        .map(_.split(","))
        .map(arr => Row(arr(0), arr(1).trim))

      val columns = "name age"

      val fields = columns.split(" ").map(col => StructField(col, StringType, false))

      val schema = StructType(fields)

      val personDs = spark.createDataFrame(fileRdd, schema)

      personDs.printSchema()
    }
    catch {
      case e: InvalidInputException => log.error("Invalid input path... \nDetails: " + e)
    }
    finally {
      SparkUtils.closeSession(spark)
    }

  }

  def udfExample(spark: SparkSession): Unit = {
    import spark.implicits._
    try {
      val studentDF = spark.read
        .option("delimiter", " ")
        .csv("/Users/ambabu/Documents/PersonalDocuments/code-samples/" +
          "PracticeProblems/Spark-Samples/src/main/resources/ds_udf_file.csv").toDF("name", "marks")
        .map(row => {
          val marks:String = if(row.get(1) != null) row.get(1).toString else "null"
          Student(row(0).toString, marks)
        })

      studentDF.show()

      val avg = (marks: String) => udfMapper.mapper(marks)

      val avg_udf = spark.udf.register("my_avg", avg)

      val studentDF_avg = studentDF.withColumn("average", avg_udf(studentDF.col("marks")))

      studentDF_avg.show()

    }
    catch {
      case e: InvalidInputException => log.error("Invalid input path... \nDetails: " + e)
    }
    finally {
      SparkUtils.closeSession(spark)
    }
  }

  object udfMapper extends Serializable {
    @transient
    private val log = LogManager.getLogger("myLogger")

    def mapper(marks: String): Option[Double] = {
      try {
        val acc = marks.split(",").map(x => x.trim.toInt).foldLeft(0, 0)((acc, v) => (acc._1 + v, acc._2 + 1))
        Some(acc._1 / acc._2.toDouble)
      } catch {
        case e: ArithmeticException =>
          log.error("Exception while calculating the average score. \nDetails: " + e)
          None
        case e: NumberFormatException =>
          log.error("Exception while calculating the average score. \nDetails: " + e)
          None
      }
    }
  }

  def udafExample(spark: SparkSession): Unit = {
    import spark.implicits._
    try {
      val studentDF = spark.read
        .option("delimiter", ",")
        .option("header", "true")
        .csv("/Users/ambabu/Documents/PersonalDocuments/code-samples/" +
          "PracticeProblems/Spark-Samples/src/main/resources/ds_udaf_file.csv")
        .map(row => {
          val mark: Double = if (row.size == 3 && row.get(2) != null) row.get(2).toString.toDouble else 0.0
          Student1(row(0).toString.toInt, row(1).toString, mark)
        })

      studentDF.show()

      spark.udf.register("UDAFAverage", MyAverage)

      studentDF.createOrReplaceTempView("StudentMark")

      val result = spark.sql("select id, UDAFAverage(mark) from StudentMark group by id")

      result.show()

    } catch {
      case e: InvalidInputException => log.error("Invalid input path... \nDetails: " + e)
    }
    finally {
      SparkUtils.closeSession(spark)
    }
  }

  object MyAverage extends UserDefinedAggregateFunction {

    @transient
    private val log = LogManager.getLogger("myLogger")

    // Data type for input arguments of this aggregate function
    override def inputSchema: StructType = StructType(StructField("mark", DoubleType) :: Nil)

    // Date type for values in the aggregation buffer
    override def bufferSchema: StructType = StructType(Seq(StructField("sum", DoubleType), StructField("count", IntegerType)))

    // Data type of the returned value
    override def dataType: DataType = DoubleType

    // whether this function will give same output on similar inputs
    override def deterministic: Boolean = true

    // Initializes the given aggregation buffer. The buffer itself is a `Row` that in addition to
    // standard methods like retrieving a value at an index (e.g., get(), getBoolean()), provides
    // the opportunity to update its values. Note that arrays and maps inside the buffer are still
    // immutable.
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0.0
      buffer(1) = 0
    }

    // Updates the given aggregate buffer with new value from input...
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if (!input.isNullAt(0)) {
        buffer(0) = buffer.getDouble(0) + input.getDouble(0)
        buffer(1) = buffer.getInt(1) + 1
      }
    }

    // Merge two aggregation buffers and store the updated value back to buffer1
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
      buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1)
    }

    override def evaluate(buffer: Row): Any = {
      try {
        Some(buffer.getDouble(0) / buffer.getInt(1))
      } catch {
        case e: ArithmeticException =>
          log.error("Exception while calculating the average score. \nDetails: " + e)
          None
        case e: NumberFormatException =>
          log.error("Exception while calculating the average score. \nDetails: " + e)
          None
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getOrCreateSparkSession("DataSetCollection", "local")
//    dsFromRdd(spark)
//    dsProgrammatically(spark)
//    udfExample(spark)
    udafExample(spark)
  }
}


