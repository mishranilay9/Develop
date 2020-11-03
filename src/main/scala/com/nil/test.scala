package com.kafka


import java.io._

import scala.collection.immutable.Map
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.parsing.json.JSON
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.text.SimpleDateFormat
import java.lang.String
import java.util.Date
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
//import java.io.file.{Files, Paths}


object test {
def main(args: Array[String]): Unit = {
  
    System.setProperty("hadoop.home.dir","C:\\Study\\hadoop")
   System.setProperty("spark.sql.warehouse.dir","file:/C:/Study/Spark/spark/spark-warehouse")
   
   val spark=SparkSession.builder().appName("TestingDBs").master("local").getOrCreate()
   val data=spark.read.textFile("C:\\Users\\Nilay\\Desktop\\nil.txt").rdd
   import spark.sqlContext.implicits._
   val df=data.toDF("HI")
   df.show()
  df.withColumn("c", substring(col("HI"), 0, 2))
  .withColumn("d", substring(col("HI"), 3, 2))
  .withColumn("e", substring(col("HI"), 5, 2))
.show()
   spark.stop()
    
   
  }
}