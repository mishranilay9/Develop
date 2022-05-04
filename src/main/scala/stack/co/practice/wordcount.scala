package stack.co.practice
import org.apache.spark.sql.SparkSession

object wordcount {
  def main(args:Array[String]){

    val spark=SparkSession.builder.appName("local").master("local").getOrCreate()
    val data=spark.read.textFile("C:\\Users\\Nilay\\Desktop\\nils.txt").rdd
    data.foreach(println)
    val res=data.flatMap(x=>x.replace(".","").split(" ")).map(x=>(x,1))
    val res1=res.groupByKey()
    print("------------------------------------\n")
    res1.foreach(println)
    spark.stop()
  }

}
