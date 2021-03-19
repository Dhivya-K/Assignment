
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
import org.apache.log4j.Logger

/**
 * Main Object to parse the URL
 * accepts the 3 arguments
 */

object Application {
  val log = Logger.getLogger(getClass.getName)

  /**
   * Main Program to run the parsing application
   * @param args
   */
  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      log.info("Necessary arguments are not found")

      throw new Exception("please provide the input and output Directory")
    }

    try {
      val constant = new Constant()
      val urlParser = new URLParser()
      val inputPath = args(0) // Input path
      val outputPath = args(1) // Output Path

      log.info("Input Path:" + inputPath)
      log.info("Output Path:" + outputPath)

      //      properties.load(new FileInputStream(proprtyFilePath))

      val appName = sys.env.getOrElse(constant.appName, "Parser")
      val master = sys.env.getOrElse(constant.master, "local[*]")
      val spark_driver_host = sys.env.getOrElse(constant.spark_driver_host, "localhost")
      
      log.info("Appname " + appName + "master" + master+ "spark driver host" +  spark_driver_host)

      val conf = new SparkConf().setMaster(master).setAppName(appName).set("spark.driver.host", spark_driver_host)
      val spark = SparkSession.builder.config(conf).getOrCreate()
      val inputSchema = spark.read.json(getClass.getResource("/input.txt").getPath()).schema
      val dfStream = spark.readStream
        .schema(inputSchema).json(inputPath)
     
      urlParser.parseMessage(dfStream, outputPath)

    } catch {
      case t: 
        Throwable => t.printStackTrace()
    }
  }

}