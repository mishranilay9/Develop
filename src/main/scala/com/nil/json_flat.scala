package com.nil
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{callUDF, struct, udf}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.SparkSession

object json_flat {
  //  def myFilterFunction(r:Row) ={
  //    var s=r.getValuesMap[Any](r.schema.fieldNames)
  //    print(s)
  //  }

  def returnNotEmptyCols(inputRow: Row): String = {
    implicit val formats = Serialization.formats(NoTypeHints)
    var colValues = inputRow.getValuesMap[Any](inputRow.schema.fieldNames)
    //.filter(x => x._2!= null && x._2!= "")
    print(inputRow.get(0)+"\n")
    var empty = new ListBuffer[Any]()
    val m = scala.collection.mutable.Map[String,Any]()
    m += "AR" -> colValues.get("number")
    m += "AZ" -> "Arizona"
    empty+= m
    val n = scala.collection.mutable.Map[String,Any]()
    n += "AR" -> "Azerbizan"
    n += "AZ" -> "Arizona"
    empty+= n
    colValues += "newF" -> empty
    Serialization.write(colValues)
  }
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("avrochk").master("local").getOrCreate()
    spark.udf.register("myFilterFunction", returnNotEmptyCols _)
    import spark.implicits._
    val df = Seq(
      (12, "bat"),
      (13, "mouse"),
      (14, "horse")
    ).toDF("number", "word")
    var dr=df.rdd.collect
    val newDF=df.withColumn("newcl",callUDF("myFilterFunction",struct(df.columns.map(df(_)) : _*)))
    newDF.show(3,0)



  }


}

