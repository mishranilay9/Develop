package com.kafka
import com.nil.convert_json_row_data.returnNotEmptyCols
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import scala.collection.mutable.ListBuffer

object test {


  def returnNotEmptyCols(a: Any): Int = {
    return a.toString.toInt+1

  }
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("avrochk").master("local").getOrCreate()
    val hoursToSecUdf = udf(returnNotEmptyCols _)
    import spark.implicits._
    val df = Seq(
      (12, "bat"),
      (13, "mouse"),
      (14, "horse")
    ).toDF("number", "word")
    val df1=df.withColumn("newg",hoursToSecUdf(df.col("number")))
    df1.show()
  }
}

