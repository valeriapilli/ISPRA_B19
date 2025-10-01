package com.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}

object Main {

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

    val df: DataFrame = spark.read.option("header", "true").csv("src/main/resources/aula/cities.csv")
    //val df: DataFrame = spark.read.option("header", "true").csv("gs://gcs-dataproc-files/input/source_data.csv")
    df.show()
    df.write.mode(SaveMode.Append).parquet("src/main/resources/aula/test_ISPRA/")
    //df.write.mode(SaveMode.Overwrite).parquet("gs://gcs-dataproc-files/output/processed_data")
  }
}
