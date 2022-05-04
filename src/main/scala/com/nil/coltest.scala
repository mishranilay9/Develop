package com.nil

import org.apache.spark.sql.SparkSession

object coltest {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("avrochk").master("local").getOrCreate()

    //val df = spark.read.format("csv").option("header", "true").load("C:\\Users\\Nilay\\Desktop\\sample.csv")


    val data=spark.read.format("orc").load("C:\\Users\\Nilay\\Desktop\\orc\\nil.orc")
    data.foreach{row=> println(row.mkString(","))}


    //val df1=df.withColumn("value",to_avro(struct(df.columns.map(col):_*)))
    //df.coalesce(1).write.format("avro").save("C:\\Users\\Nilay\\Desktop\\avro")

  }


}
