package com.spark

import org.apache.spark.SparkContext

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object Main {

  def main (arg:Array[String]): Unit = {

    ArgsValidator.checkArgs(arg)
    val input = ArgsValidator.input //"gs://gcs-dataproc-files/input/"
    val output = ArgsValidator.output //"gs://gcs-dataproc-files/output/processed_data/"

  val spark = SparkSession.builder()
    //commentare l'opzione del master prima di eseguire la build
    .master("local[1]")
    .appName("Test_B19")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  //val sqlContext:SQLContext = spark.sqlContext


  //commenta se vuoi vedere i log spark
  //sc.setLogLevel("ERROR")
    sc.setLogLevel("INFO")

    val ruvReader = new RuvReader(spark, sc, input)

    //creazione del DF finale con 1 solo file
    //val cleanData: DataFrame = ruvReader.createDF(ruvReader.readRUV("RDLm_BARK_2025_03_12_0000.ruv"))//ruvReader.createDFTotal()

    //creazione del DF finale con più file
    val cleanData: (DataFrame, String) = ruvReader.createDFTotal()

    //scrittura del DataFrame sul file CSV (singolo file)
   //cleanData.write.mode(SaveMode.Overwrite).option("Header","True").csv(output + "BARK")
    //scrittura del DataFrame sul file CSV (più file)
    cleanData._1.write.mode(SaveMode.Overwrite).option("Header","True").csv(output + cleanData._2)

}
}

