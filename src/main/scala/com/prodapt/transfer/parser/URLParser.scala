package com.prodapt.transfer.URLParser
import com.prodapt.transfer.constant.Constant
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.FloatType
import java.util.regex.Pattern
import org.apache.log4j.Logger

/**
 *
 * Class to parse the URL
 * as mentioned in the assignment
 *
 */

class URLParser {
  val log = Logger.getLogger(getClass.getName)

  /**
   * Method to parse the url
   * @param filteredRDD
   * @param outputDir
   */
  def parseMessage(dfStream: org.apache.spark.sql.DataFrame, outputPath: String) = {
    log.info("***** parse Message ****** ")
    log.info("Output Path: " + outputPath)
    try {
      val constant = new Constant()

      val filteredDf = dfStream.withColumn("filtered", dfStream.col("message").rlike("http://omwssu"))

      val messageCol = filteredDf.col(constant.message_col)
      val regex_extract_ts = regexp_extract(messageCol, constant.timestampREgex, 0)

      //parse the df
      val extracted_df = filteredDf.na.drop("all").
        withColumn(constant.timestamp, date_format(to_timestamp(regexp_extract_by_grp_num(messageCol, lit(constant.timestampREgex), lit(0)), constant.timestamp_source), constant.timestamp_format))
        .withColumn(constant.url, regexp_extract_by_grp_num(messageCol, lit(constant.urlRegex), lit(0)))
        .withColumn(constant.cpe_id, regexp_extract_by_grp_num(messageCol, lit(constant.urlRegex), lit(3)))
        .withColumn(constant.fqdn, regexp_extract_by_grp_num(messageCol, lit(constant.urlRegex), lit(2)))
        .withColumn(constant.action, regexp_extract_by_grp_num(messageCol, lit(constant.urlRegex), lit(4)))
        .withColumn(constant.error_code, regexp_replace(regexp_extract_by_grp_num(messageCol, lit(constant.urlRegex), lit(5)), constant.front_slash, constant.dot).cast(FloatType))
      val col_names: List[String] = List(constant.timestamp, constant.url, constant.error_code, constant.fqdn, constant.action, constant.cpe_id)

      //drop the null values and save it in a file.Used appending to have the file for future
      val outputDF = extracted_df.select(col_names.head, col_names.tail: _*).withColumnRenamed(constant.url, constant.message_col).na.drop("all")

      //write Stream
      outputDF.coalesce(constant.numPartition).writeStream.outputMode(constant.outputMode).format("json").option("checkpointLocation", constant.checkpointLocation)
      .start(outputPath).awaitTermination()
    } catch {
      case t: Throwable => t.printStackTrace()

    }
  }

  /**
   * udf fo pattern Match
   * @return
   */
  def regexp_extract_by_grp_num = udf((job: String, exp: String, groupIdx: Int) => {

    val pattern = Pattern.compile(exp.toString)

    val m = pattern.matcher(job.toString)
    var result = Seq[String]()
    while (m.find) {
      val temp =
        result = result :+ m.group(groupIdx)
    }
    result.mkString(",")
  })

}
