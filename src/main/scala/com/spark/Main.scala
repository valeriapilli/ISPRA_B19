package com.spark

import org.apache.spark.SparkContext

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import com.spark.ArgsValidator
object Main extends Serializable {

  def main (arg:Array[String]): Unit = {

    ArgsValidator.checkArgs(arg)
    val input = ArgsValidator.input
    val output = ArgsValidator.output

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("Test_B19")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  //val sqlContext:SQLContext = spark.sqlContext


  //commenta se vuoi vedere i log spark
  //sc.setLogLevel("ERROR")
    sc.setLogLevel("INFO")

    val ruvReader = new RuvReader(spark, sc, input)

    val cleanData: (DataFrame, String) = ruvReader.createDF()

    //scrittura del DataFrame sul file CSV
   cleanData._1.write.mode(SaveMode.Overwrite).option("Header","True").csv(output + cleanData._2)

}
}

