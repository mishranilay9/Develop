package com.nil

import java.io._
import scala.collection.immutable.Map
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.parsing.json.JSON
object Flatten_json_using_scala {




  def flattenMap(m: Map[String, Any], tree: String="") : Iterable[(String, Any)] = m.flatten {
    case (k: String, v: Map[String, Any] @unchecked) => flattenMap(v, k)
    case (k: String, v: List[Map[String, Any]] @unchecked) => v.flatten(flattenMap(_, k))
    case (k: String, v: Any) => List((k.toString).mkString("") ->v)
    case (k,null) => List((k.toString).mkString("_") ->"null")
  }

  """
    |def flattenMap(m: Map[String, Any], tree: List[String] = List()) : Iterable[(String, Any)] = m.flatMap {
    |      case (k: String, v: Map[String, Any]) => flattenMap(v, tree :+ k)
    |      case (k: String, v: List[Any]) if v.headOption.exists(_.isInstanceOf[Map[String, Any]]) => v.flatMap{ subNode =>
    |        Simply(subNode.asInstanceOf[Map[String, Any]], tree :+ k)
    |      }
    |      case (k: String, v: List[String]) => List((tree :+ k.toString).mkString("_") -> v.mkString(","))
    |      case (k: String, v: Any) => List((tree :+ k.toString).mkString("_") ->v)
    |      case (k,null) => List((tree :+ k.toString).mkString("_") ->"null")
    |
    |    }
    |"""



  def main(args: Array[String]): Unit = {



    val jsonString = Source.fromFile("C:\\Users\\Nilay\\Desktop\\vld.json").getLines.mkString
    val jsonMap: Map[String, Any] = JSON.parseFull(jsonString).get.asInstanceOf[Map[String, Any]]




    val itrable1=flattenMap(jsonMap).toSeq
    val c=(itrable1.map(x=>x._1)).mkString(",")
    val d=itrable1.map(x=>s""""${x._2}"""")


    val file = new File("C:\\Users\\Nilay\\Desktop\\Test1.csv")
    val bw1 = new BufferedWriter(new FileWriter(file))
    bw1.write(c+System.lineSeparator())
    bw1.write(d.mkString(","))
    bw1.close()





  }

}

