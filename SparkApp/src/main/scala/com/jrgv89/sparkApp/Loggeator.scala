package com.jrgv89.sparkApp

import org.slf4j
import org.slf4j.LoggerFactory

//object SinfoLogLevel extends Enumeration {
//  val ALL, TRACE, DEBUG, INFO, WARN, ERROR, FATAL, OFF = Value
//}
//
//object SinfoLogInstance extends Enumeration {
//  val BOTH, DRIVER, EXECUTOR = Value
//}

trait Loggeator {
  private def isDebug: Boolean = SparkAppConfig.instance.isDebug

  def clazz : Class[_]

  val loggeator: slf4j.Logger = LoggerFactory.getLogger(clazz.getName)

  //  def sLog(level: SinfoLogLevel.Value, data: Any, instance: SinfoLogInstance.Value = SinfoLogInstance.DRIVER): Unit = {
  ////    println(data.toString)
  //    instance match {
  //      case SinfoLogInstance.BOTH =>
  //        sLogDriver(level, data)
  //        sLogExecutor(level, data)
  //      case SinfoLogInstance.DRIVER => sLogDriver(level, data)
  //      case SinfoLogInstance.EXECUTOR => sLogExecutor(level, data)
  //      case _ =>
  //    }
  //  }
  //
  //  def sLogDriver(level: SinfoLogLevel.Value, data: Any): Unit = {
  //    println(data.toString)
  //    level match {
  //      case SinfoLogLevel.ALL => log.error(data.toString)
  //      case SinfoLogLevel.TRACE => log.trace(data.toString)
  //      case SinfoLogLevel.DEBUG => log.debug(data.toString)
  //      case SinfoLogLevel.INFO => log.info(data.toString)
  //      case SinfoLogLevel.WARN => log.warn(data.toString)
  //      case SinfoLogLevel.ERROR =>
  //      case SinfoLogLevel.FATAL => log.error(data.toString)
  //      case _ =>
  //    }
  //  }
  //
  //  def sLogExecutor(level: SinfoLogLevel.Value, data: Any): Unit = {
  //    println(data.toString)
  //    level match {
  //      case SinfoLogLevel.ALL => logError(data.toString)
  //      case SinfoLogLevel.TRACE => logTrace(data.toString)
  //      case SinfoLogLevel.DEBUG => logDebug(data.toString)
  //      case SinfoLogLevel.INFO => logInfo(data.toString)
  //      case SinfoLogLevel.WARN => logWarning(data.toString)
  //      case SinfoLogLevel.ERROR =>
  //      case SinfoLogLevel.FATAL => logError(data.toString)
  //      case _ =>
  //    }
  //  }

  //  def sShow(name: String, df: DataFrame, truncate:Boolean=true, number:Int=20): Unit = {
  //    if (isDebug) {
  //      println("**************")
  //      println(s"* $name")
  //      println("**************")
  //      df.show(number, truncate)
  //    }
  //  }
}
