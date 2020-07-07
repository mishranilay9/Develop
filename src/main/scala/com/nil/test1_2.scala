package com.nil
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{collect_list, struct}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SparkSession
object test1_2 {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("avrochk").master("local").getOrCreate()
    import spark.implicits._
    val df = spark.read.format("csv").option("header", "true").load("C:\\Users\\Nilay\\Desktop\\sample.csv")
    //df.show()
    df.coalesce(1).write.format("com.databricks.spark.avro").save("C:\\Users\\Nilay\\Desktop\\avro")

      //.withColumn("accounts",
       // struct($"acct_no", $"customers"))
      //.drop("customers")
    //val df2=df1.select("accounts").rdd.flatMap(x=>named_struct('hey',x,'ity','ind'))
    //df1.select($"accounts").map(x=>named_struct(x)).show(false)
  }


}
