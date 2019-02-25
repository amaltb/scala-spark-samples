name := "Spark-Samples"

version := "0.1"

scalaVersion := "2.11.4"

libraryDependencies ++= Seq (
  "org.apache.spark" % "spark-core_2.11" % "2.3.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.3.0",
  "log4j" % "log4j" % "1.2.17",
  "com.jcabi" % "jcabi-log" % "0.17.3"
)
