package com.kafka

import java.util.Properties

import org.apache.kafka.clients.producer._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType,StructField, StructType,FloatType}

import scala.util.parsing.json.JSON
object producer_spark {
  def main(args: Array[String]): Unit = {
    writeToKafka("quick-start")
  }
  def writeToKafka(topic: String): Unit = {
    val spark = SparkSession.builder().appName("avrochk").master("local").getOrCreate()

    val mySchema = StructType(Array(
      StructField("transaction_status", StringType),
      StructField("amount", FloatType),
      StructField("category", StringType),
      StructField("email_id", StringType),
      StructField("ciskey", IntegerType)
    ))
    val streamingDataFrame = spark.readStream.schema(mySchema).csv("C:\\Users\\Nilay\\Desktop\\sample.csv")
    streamingDataFrame.selectExpr("CAST(ciskey AS STRING) AS key", "to_json(struct(*)) AS value").
      writeStream
      .format("kafka")
      .option("topic", "test")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("checkpointLocation","C:\\Study\\kafka\\Studyapache-zookeeperlog")
      .start()
  }
}