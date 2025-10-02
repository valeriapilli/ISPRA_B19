package com.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
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
  sc.setLogLevel("ERROR")

val ruv: RDD[String] = sc.textFile(input)//"gs://gcs-dataproc-files/input/RDLm_BARK_2025_03_12_0000.ruv")
 // spark.read.option("Header","True").csv("gs://gcs-dataproc-files/input/source_data.csv")
   // .write.parquet("gs://gcs-dataproc-files/ouput/processed_data.parquet")

   // ruv.take(100).foreach(println)
val site: String = ruv.filter(line => line.startsWith("%Site")).first()


  val site1 =  site.replace("%Site: ", "").replace(" \"\"", "")
    println(site1)
  //.foreach(println)
    //ruv.map(x => x.concat(x.startsWith("%").toString)).
      //take(100).foreach(println)
    val tms: String = ruv.filter(line => line.startsWith("%TimeStamp: ")).first()
    val tms1 = tms.replace("%TimeStamp: ", "").split("  ")


   // val tms: Array[String] = "2025 03 12  00 00 00".split("  ")
    val time_format = tms1(1).replace(" ", ":")
    val tms_format = tms1(0).replace(" ", "-").concat("T").concat(time_format)
    println(time_format+ "\n"+tms_format)

    val meas: RDD[String] = ruv.filter(line => !line.startsWith("%"))
    //meas.take(50).foreach(println)


    println("Numero di righe file originale: " +ruv.count())
    println("Numero di righe che iniziano con %: " +ruv.filter(line => line.startsWith("%")).count())
    println("Numero di righe che NON iniziano con %: "+meas.count())

    val rdd = meas.map(_.trim.split("\\s+")).map(a => Row.fromSeq(a))
    //rdd.take(20).foreach(println)

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

val data = spark.createDataFrame(rdd,schema).select("Longitude", "Latitude", "Velocity","Direction")
  .withColumn("Timestamp", lit(tms_format)).withColumn("Time", lit(time_format)).withColumn("Site", lit(site1))
    data.show()

    data.write.mode(SaveMode.Overwrite).option("Header","True").csv(output+site1)//"gs://gcs-dataproc-files/output/"+site1)




}
}

