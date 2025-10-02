package com.spark


import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}

object Test {

  def main (arg: Array[String]): Unit ={
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("test_job")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    //val sqlContext:SQLContext = spark.sqlContext

    //commenta se vuoi vedere i log spark
    sc.setLogLevel("ERROR")

    println("First SparkContext:")
    println("APP Name :" + spark.sparkContext.appName)
    println("Deploy Mode :" + spark.sparkContext.deployMode)
    println("Master :" + spark.sparkContext.master)

    /*val x = sc.textFile("src/main/resources/aula/phone.csv")
  println(x.count())
  */

    //val df: DataFrame = spark.read.option("header", "true").csv("src/main/resources/aula/cities.csv")
    //val df: DataFrame = spark.read.option("header", "true").csv("gs://gcs-dataproc-files/input/source_data.csv")
    //df.show()
    //df.write.mode(SaveMode.Append).parquet("src/main/resources/aula/test_ISPRA/")
    //df.write.mode(SaveMode.Overwrite).parquet("gs://gcs-dataproc-files/output/processed_data")


    /*
    val tms: Array[String] = "2025 03 12  00 00 00".split("  ")
    val time_format = tms(1).replace(" ", ":")
    val tms_format = tms(0).replace(" ", "-").concat("T").concat(time_format)
    println (time_format + "\n" + tms_format)

    val x: Boolean = !time_format.startsWith("%")
    println(x)
*/
    val str = "1   4  f   t      y   u "
    val ar = str.split("\\s+")
    ar.foreach(println)
    println(ar.length)

    //.map(a => Row.fromSeq(a))
  }
}
