package com.spark

import org.apache.hadoop.fs.{FileStatus, FileSystem, LocatedFileStatus, Path, RemoteIterator}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class RuvReader (spark: SparkSession, sc: SparkContext, inputPath: String) {

  def readRUV(fileName:String): RDD[String] = {

    val ruvRDD: RDD[String] = sc.textFile(this.inputPath + fileName)
    ruvRDD
  }

  //val rddData = readRUV()

  def getSite(rddData: RDD[String]): String = {
    val site = rddData.filter(line => line.startsWith("%Site")).first()
    val site_format =  site.replace("%Site: ", "").replace(" \"\"", "")
    println(site_format)

    site_format
  }

  def getTimestamp(rddData: RDD[String]): Array[String] = {
    val tms: Array[String] = rddData.filter(line => line.startsWith("%TimeStamp: ")).first().replace("%TimeStamp: ", "").split("  ")

    val time_format = tms(1).replace(" ", ":")
    val tms_format = tms(0).replace(" ", "-").concat("T").concat(time_format)
    println("Timestamp: " + time_format+ "\n" + "Ora: "+tms_format)

    Array(tms_format,time_format)
  }

  def createDF(rddData: RDD[String]): DataFrame = {
    val site = getSite(rddData)
    val tms = getTimestamp(rddData)
    val meas: RDD[Row] = rddData.filter(line => !line.startsWith("%")).
      map(_.trim.split("\\s+")).map(a => Row.fromSeq(a))

    val schema = createSchema()

    val cleanData = spark.createDataFrame(meas,schema).select("Longitude", "Latitude", "Velocity","Direction")
      .withColumn("Timestamp", lit(tms(0))).
      withColumn("Time", lit(tms(1))).
      withColumn("Site", lit(site))
    cleanData.show()

    cleanData
  }

  def getAllFiles: Array[String] = {
    val path = new Path(inputPath)
    val fs: FileSystem = path.getFileSystem(sc.hadoopConfiguration)

    val statusArr: Array[FileStatus] = fs.listStatus(path)

    val fileArr = statusArr.
      filter(status => status.isFile).
      map(file => file.getPath.getName).
      filter (name => name.endsWith(".ruv"))

    fileArr.foreach { fileName =>
      println("Nome File: " + s"${fileName} ")
    }
    fileArr
  }

  def createDFTotal(): (DataFrame, String) = {

    val arrFile: Array[String] = getAllFiles
    val sito = arrFile(0).split("_")(1) //RDLm_BARK_2025_03_12_0100
    println("SITO: " + sito)
    //var listDF = List.empty[(DataFrame, String)]

    var totalDF: DataFrame = spark.createDataFrame(
      spark.sparkContext.emptyRDD[Row],
      createSchemaClean()
    )

    for (file <- arrFile) {
      val singleFileRDD: RDD[String] = readRUV(file)
      val singleFileDF: DataFrame = createDF(singleFileRDD)
      totalDF = totalDF.unionByName(singleFileDF)
    }

    (totalDF, sito)

  }

  def createSchema() = {
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

    schema
  }

  def createSchemaClean() = {
    val schema = StructType(Array(
      StructField("Longitude", StringType, true),
      StructField("Latitude", StringType, true),
      StructField("Velocity", StringType, true),
      StructField("Direction", StringType, true),
      StructField("Timestamp", StringType, true),
      StructField("Time", StringType, true),
      StructField("Site", StringType, true)
    ))

    schema
  }



}
