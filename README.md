# Assignment

A simple scala application to parse the url from the given text file using spark streaming.

To Run the application 

spark-submit --class com.prodapt.transfer.Application tranfer.jar-with-dependencies <propertyfile> <inputPath> <OutputPath>
  
  
Property file Should have these parameters
spark.driver.host = localhost
Master=local[*]
AppName=transfer

change accordingly based on the configuration

Prerequisites :

 Java (JDK) :1.8
 Scala : 2.11
 Spark : 2.4.5
 
Make sure to set the HADOOP_HOME variable

Build jar using maven. 

