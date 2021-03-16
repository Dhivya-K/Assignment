
package com.prodapt.transfer

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import com.prodapt.transfer.URLParser.URLParser
import scala.io.Source
import scala.util.Properties
import java.util.Properties
import scala.collection.JavaConverters._
import java.io.FileInputStream
import com.prodapt.transfer.constant.Constant

/**
 * Main Object to parse the URL
 * accepts the 3 arguments
 */

object Application {
  
/**
 * Main Program to run the parsing application
 * @param args
 */
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      throw new Exception("please provide the input and output Directory")
    }
    try{ 
      val constant = new Constant()
    val inputPath = args(1) // Input path
    val outputPath = args(2) // Output Path
    val proprtyFilePath = args(0) // Property File

    val properties = new Properties

    properties.load(new FileInputStream(proprtyFilePath))

    val appName = properties.getProperty(constant.appName)
    val master = properties.getProperty(constant.master)
    val spark_driver_host = properties.getProperty(constant.spark_driver_host)

    val conf = new SparkConf().setMaster(master).setAppName(appName).set("spark.driver.host", spark_driver_host)
    //    val spark = SparkSession.builder.config(conf).getOrCreate()

    val ssc = new StreamingContext(conf, Seconds(60))
    println("test after spark initialization")
    //
    //    //returns DataFrame
    //    val sourceDir: String = "file:\\D:\\prodapt\\inuts"
    val lines = ssc.textFileStream(inputPath)
    val filterURl = "http://omwssu."
    val filteredRDD = lines.filter(_.contains(filterURl))

    val urlParser = new URLParser()
    urlParser.parseMessage(filteredRDD, outputPath)

    ssc.start()
    ssc.awaitTermination()
    }catch {
      case t: Throwable => t.printStackTrace() 
    }
  
  }

}