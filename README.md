# Assignment

A simple scala application to parse the url from the given text file using spark structured streaming API.

To Run the application 

spark-submit --class com.prodapt.transfer.Application tranfer.jar-with-dependencies <inputPath> <OutputPath>
  
  
set The below Environmental variables.
spark.driver.host = localhost
Master=local[*]
AppName=transfer

change the above accordingly based on the configuration



Prerequisites :

 Java (JDK) :1.8
 Scala : 2.11
 Spark : 2.4.5
 
Make sure to set the HADOOP_HOME variable

Build jar using maven. 

