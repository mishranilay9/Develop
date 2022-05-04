package com.logger.imple

import com.logger.imple.LogginFactory
import ch.qos.logback.classic.Logger
class StreamLoggerFactory(appId:String,appName:String,rootdir:String) (implicit className:Class[_])
  extends LogginFactory(appId:String,appName:String,rootdir:String,className:Class[_]){

  def getLogger():Logger ={
    getFileLogger()
  }

}

object LogTest extends App {
  implicit val className:Class[_]=this.getClass
  print(className.getName)
  val logger = new LogginFactory("a00ad3","Test","C:\\Users\\Nilay\\Desktop",this.getClass).getFileLogger()
  logger.info("Hi There I am Nikay")
  logger.debug("HI There debug log")
  logger.warn("This is war msg")
  logger.error("This is war msg")
}

