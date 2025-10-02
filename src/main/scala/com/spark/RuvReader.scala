package com.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class RuvReader (spark: SparkSession, sc: SparkContext, inputPath: String) extends Serializable {

  def readRUV(): RDD[String] = {

    val ruvRDD: RDD[String] = sc.textFile(this.inputPath)
    ruvRDD
  }

  val rddData = readRUV()

  def getSite(): String = {
    val site = rddData.filter(line => line.startsWith("%Site")).first()
    val site_format =  site.replace("%Site: ", "").replace(" \"\"", "")
    println(site_format)

    site_format
  }

  def getTimestamp(): Array[String] = {
    val tms: Array[String] = rddData.filter(line => line.startsWith("%TimeStamp: ")).first().replace("%TimeStamp: ", "").split("  ")

    val time_format = tms(1).replace(" ", ":")
    val tms_format = tms(0).replace(" ", "-").concat("T").concat(time_format)
    println("Timestamp: " + time_format+ "\n" + "Ora: "+tms_format)

    Array(tms_format,time_format)
  }

  def createDF(): (DataFrame, String) = {

    val site = this.getSite()
    val meas: RDD[Row] = rddData.filter(line => !line.startsWith("%")).
      map(_.trim.split("\\s+")).map(a => Row.fromSeq(a))

    val schema = StructType(Array(
      StructField("Longitude", StringType, true),
      StructField("Latitude", StringType, true),
      StructField("U_comp", StringType, true),
      StructField("V_comp", StringType, true),
      StructField("VectorFlag", StringType, true),
      StructField("Spatial_quality", StringType, true),
      StructField("Temporal_quality", StringType, true),
      StructField("Velocity_max", StringType, true),
      StructField("Velocity_min", StringType, true),
      StructField("Spatial_cnt", StringType, true),
      StructField("Temporal_cnt", StringType, true),
      StructField("X_Distance", StringType, true),
      StructField("Y_Distance", StringType, true),
      StructField("Range", StringType, true),
      StructField("Bearing", StringType, true),
      StructField("Velocity", StringType, true),
      StructField("Direction", StringType, true),
      StructField("Spectra_rngCell", StringType, true)))

    val cleanData = spark.createDataFrame(meas,schema).select("Longitude", "Latitude", "Velocity","Direction")
      .withColumn("Timestamp", lit(this.getTimestamp()(0))).
      withColumn("Time", lit(this.getTimestamp()(1))).
      withColumn("Site", lit(site))
    cleanData.show()

    (cleanData, site)
  }



}
