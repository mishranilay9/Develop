package com.nil

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, collect_list, _}

object test {


  //def addOne(elements: Seq[String]):Any = return elements.map(element => ("id",s"CIS-$element","CISKey","Ind"))
  def addOne(elements: Seq[String]) = elements.map(element => (List("id",s"CIS-$element","CISKey","Ind").grouped(2).collect { case List(k, v) => k -> v }.toMap))


  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("avrochk").master("local").getOrCreate()
    val plusOneInt = spark.udf.register("plusOneInt", addOne(_: Seq[String]))
    //val test=spark.udf.register("listToMap",listToMap[T](xs: Seq[String]))
    import spark.implicits._
    val df = spark.read.format("csv").option("header", "true").load("C:\\Users\\Nilay\\Desktop\\sample.csv")
    val groupBy = df.columns.filter(_!="ciskey")
    //val df2=df.groupBy(groupBy.map(col): _*).agg(collect_list($"ciskey").as("accounts"))
      //.withColumn("customers", expr("TRANSFORM(accounts, " +
        //"x -> named_struct('ciskey_no', x, 'ciskey_val', 'IND'))"))
      //.withColumn("accounts",
        //struct($"acct_no", $"customers"))
      //.drop("customers")
    val df2=df.groupBy(groupBy.map(col): _*).agg(collect_list($"ciskey").as("accounts"))
          .withColumn("customers", plusOneInt($"accounts"))
    .withColumn("accounts", struct($"acct_no", $"customers"))
    .drop("customers")
    //df2.printSchema()
    df2.coalesce(1).write.json("C:\\Users\\Nilay\\Desktop\\json1")



  }
}
