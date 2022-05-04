package com.nil
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{collect_list, struct}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit
object test1 {
  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("avrochk").master("local").getOrCreate()
    import spark.implicits._
    val data = spark.read.format("csv").option("header", "true").load("C:\\Users\\Nilay\\Desktop\\sample.csv")
    val data2=data.withColumn("Id_Type",lit("CISKEY"))
    val data3=data2.withColumnRenamed("ciskey","Id")
    val data4=data3
      .groupBy($"unique_id")
      .agg(
        collect_set(
          struct(
            $"transaction_status",
            $"amount",
            $"category",
            $"email_id",
            $"unique_id",
            $"acct_no"
          )).as("json_data"),

        first("acct_no").as("acct_no"),
        (collect_list(struct(concat(lit("CIS-"),$"id").alias("Id"),$"Id_Type")).as("customers"))
      )

      .withColumn("json_data",explode($"json_data"))
      .withColumn("accounts",struct($"acct_no",$"customers"))
      .select($"json_data.*",$"accounts")
     //data4.coalesce(1).write.json("C:\\Users\\Nilay\\Desktop\\json")
    data4.coalesce(1).write.format("com.databricks.spark.avro").save("C:\\Users\\Nilay\\Desktop\\avro")
      //data4.write.format("avro").save("C:\\Users\\Nilay\\Desktop\\avro")

  }
}
