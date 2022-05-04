package com.nil

object caseTest {

  def eithTest(a:Int,b:Int):Either[String,Int]={
    if (b==0) Left("Devide by zero exception")
    else Right(a/b)
  }

  def main(args: Array[String]): Unit = {
    var s:Either[Any,Any] = eithTest(1,0)
    print(s)
    eithTest(1,0) match {
      case Right(s) => println(s)
      case Left(i) => println(i)
    }

  }

}
