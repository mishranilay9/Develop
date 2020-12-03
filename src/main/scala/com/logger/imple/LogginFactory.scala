package com.logger.imple

import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.{Logger,LoggerContext}
import ch.qos.logback.core.encoder.Encoder
import ch.qos.logback.core.rolling.{RollingFileAppender,TimeBasedRollingPolicy}
import ch.qos.logback.core.util.FileSize
import ch.qos.logback.core.{ConsoleAppender,FileAppender}



class LogginFactory(appId:String,appName:String,rootdir:String,className:Class[_]) {

  //val pattern=s"%d{yyyy-MM-dd HH:mm:ss.SSS} %-5level CalledFrom: $appId-$appName-$className [%thread] %logger{36} - %msg%n"
  val pattern=s"%d{yyyy-MM-dd HH:mm:ss.SSS} %-5level CalledFrom: $appId-$appName-$className [%thread] %logger{36} - %msg%n"
//  val rootLogger=new LoggerContext().getLogger("ROOT")
//  val rootLoggerContext=rootLogger.getLoggerContext
//  print(rootLogger.getName)

  val fileLogger=new LoggerContext().getLogger("FileLogger")
  val fileLoggerContext=fileLogger.getLoggerContext
  print(fileLogger.getName)

  def getFileLogger():Logger = {
    val fileAppender=_getFIleAppender()
    fileAppender.start()
    fileLogger.addAppender(fileAppender.asInstanceOf[FileAppender[ILoggingEvent]])
    fileLogger
  }
  private def _getFIleAppender():FileAppender[PatternLayoutEncoder]={
    val fileAppender=new FileAppender[PatternLayoutEncoder]
    fileAppender.setContext(fileLoggerContext)
    val patternEncoder=_getPatternEncoder(fileLoggerContext)
    patternEncoder.start()
    fileAppender.setEncoder(patternEncoder.asInstanceOf[Encoder[PatternLayoutEncoder]])
    fileAppender.setFile(rootdir+"/"+s"${appId}-${appName}-output.log")
    fileAppender
  }

  private def _getPatternEncoder(loggerContext: LoggerContext):PatternLayoutEncoder={
    val patternEncoder:PatternLayoutEncoder = new PatternLayoutEncoder
    patternEncoder.setContext(loggerContext)
    patternEncoder.setPattern(pattern)
    patternEncoder
  }
}
