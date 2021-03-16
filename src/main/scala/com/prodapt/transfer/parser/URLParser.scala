package com.prodapt.transfer.URLParser
import com.prodapt.transfer.constant.Constant
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.FloatType

/**
 *
 * Class to parse the URL
 * as mentioned in the assignment
 *
 */

class URLParser {

  /**
   * Method to parse the url
   * @param filteredRDD
   * @param outputDir
   */
  def parseMessage(filteredRDD: org.apache.spark.streaming.dstream.DStream[String], outputDir: String) = {
    try {
      filteredRDD.foreachRDD { rdd =>
        //initialize spark
        val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
        //convert to DF
        val df = spark.read.json(rdd)
        //cache the df to speed up
        df.cache()
        if (!df.isEmpty) {
          if (df.columns.contains("message")) {
            val constant = new Constant()
            val messageCol = df.col(constant.message_col)
            val regex_extract_ts = regexp_extract(messageCol, constant.timestampREgex, 0)

            val regex_extract_col = regexp_extract(messageCol, constant.urlRegex, 1)
            val regex_extract_fqdn = regexp_extract(messageCol, constant.urlRegex, 2)

            val regex_extract_cpe_id = regexp_extract(messageCol, constant.urlRegex, 3)

            val regex_extract_action = regexp_extract(messageCol, constant.urlRegex, 4)
            val regex_extract_dd = regexp_extract(messageCol, constant.urlRegex, 5)

            //parse the df
            val extracted_df = df.withColumn(constant.url, regex_extract_col).
              withColumn(constant.timestamp, date_format(to_timestamp(regex_extract_ts, constant.timestamp_source), constant.timestamp_format))
              .withColumn(constant.cpe_id, regex_extract_cpe_id).
              withColumn(constant.fqdn, regex_extract_fqdn).withColumn(constant.action, regex_extract_action).
              withColumn(constant.error_code, (regexp_replace(regex_extract_dd, constant.front_slash, constant.dot).cast(FloatType)))
            val col_names: List[String] = List(constant.timestamp, constant.url, constant.error_code, constant.fqdn, constant.action, constant.cpe_id)

            //drop the null values and save it in a file.Used appending to have the file for future
            val outputDF = extracted_df.select(col_names.head, col_names.tail: _*).withColumnRenamed(constant.url, constant.message_col).na.drop("all")

            outputDF.coalesce(1).write.mode(SaveMode.Append).json(outputDir)
          }
        }
      }
    } catch {
      case t: Throwable => t.printStackTrace()
    }
  }
}

