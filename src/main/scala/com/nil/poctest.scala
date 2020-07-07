package com.nil
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{collect_list, struct}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
object poctest {


    def flattenStruct(schema: StructType): Array[Column] = schema.fields.flatMap(f => {

      f.dataType match {
        case st: StructType => flattenStruct(st)

      }
    })

    def flattenSchema(df: DataFrame): DataFrame = {
      val structExists = df.schema.fields.filter(_.dataType.typeName == "struct").size > 0
      val arrayCols = df.schema.fields.filter(_.dataType.typeName == "array").map(_.name)

      if (structExists) {
        flattenSchema(df.select(flattenStruct(df.schema): _*))

      } else {
        df
      }
    }

    def main(args: Array[String]) {

      val spark = SparkSession.builder().appName("avrochk").master("local").getOrCreate()
      import spark.implicits._
      val data = spark.read.format("csv").option("header", "true").load("C:\\Users\\Nilay\\Desktop\\sample.csv")
      //data.show()
      val data1 = data.groupBy($"unique_id")
        .agg(collect_list(array($"acct_no",$"ciskey")).as("accounts")).groupBy($"accounts".getItem(0).getItem(0).alias("acct_no")).agg((collect_list($"accounts")))
      val data2=data1.groupBy($"accounts".getItem(0).getItem(0).alias("acct_no")).agg((collect_list($"accounts")))
      //val data2=data1.withColumn("newt",$"accounts".getItem(0).getItem(0))
      data2.show(false)






      //val data3 = data.groupBy($"unique_id")//,$"acct_no")
        //.agg(collect_set($"ciskey").alias("accounts"))
        //val data2 = data1.withColumn("test1", explode($"accounts")).withColumn("test2",explode($"test1")).drop("accounts")
      //data1.coalesce(1).write.json("C:\\Users\\Nilay\\Desktop\\json")
     //val resultDf = data.join(data3, Seq("unique_id","acct_no")).dropDuplicates()
      //resultDf.show()
      //resultDf.coalesce(1).write.json("C:\\Users\\Nilay\\Desktop\\json")




    }


}
