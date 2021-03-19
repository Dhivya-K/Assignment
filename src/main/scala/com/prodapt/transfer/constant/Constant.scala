package com.prodapt.transfer.constant

class Constant {
          val appName = "AppName"
          val master = "Master"
          val spark_driver_host = "spark.driver.host"
          val urlRegex = "((http://omwssu.*\\.com/[a-z]*)/([\\w]*-\\w*-\\d{9})/([a-z]*)/(\\d/\\d)/[\\w\\%]*)"
          val timestampREgex = "([0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])\\W+(2[0-3]|[01][0-9]):[0-5][0-9]:([0-5][1-9]))"
          val timestamp = "timestamp"
          val cpe_id = "cpe_id"
          val fqdn = "fqdn"
          val action = "action"
          val message_col = "message"
          val error_code = "error_code"
          val timestamp_format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
          val url = "url"
          val front_slash = "/"
          val dot = "."
          val timestamp_source = "yyyy-MM-dd HH:mm:ss"
          val outputMode = "Append"
          val checkpointLocation = "checkpoint"
          val numPartition = 1
}