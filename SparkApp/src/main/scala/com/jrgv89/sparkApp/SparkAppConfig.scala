package com.jrgv89.sparkApp

import com.jrgv89.sparkApp.exceptions.{MandatoryKeyNotFound, MandatoryKeyNotFoundErrors}
import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration

import scala.collection.JavaConverters._

/**
  * Created by JRGv89 on 20/05/2017.
  */
private[sparkApp] class SparkAppConfig(options : Config) extends Constants {



  var isDebug: Boolean = false
  var isHdfsEnabled: Boolean = true
  var isLoggerEnabled: Boolean = true
  var hdfsConfiguration: Configuration = new Configuration()


  if(options.hasPath("hdfs")) parseHDFSConfig(options.getConfig("hdfs"))
  if(options.hasPath("debug")) parseDebugConfig(options.getConfig("debug"))
  if(options.hasPath("logger")) parseLoggerConfig(options.getConfig("logger"))


  private def parseLoggerConfig(loggerConfig: Config): Unit = {
    //TODO
    if(!loggerConfig.hasPath(ENABLE)) throw new MandatoryKeyNotFound(MandatoryKeyNotFoundErrors.mandatoryKeyNotFoundError.code,
      MandatoryKeyNotFoundErrors.mandatoryKeyNotFoundError.message.format(ENABLE, "options.logger.enable"))
    isLoggerEnabled = loggerConfig.getBoolean(ENABLE)
  }

  private def parseDebugConfig(debugConfig: Config): Unit ={
    if(!debugConfig.hasPath(ENABLE)) throw new MandatoryKeyNotFound(MandatoryKeyNotFoundErrors.mandatoryKeyNotFoundError.code,
      MandatoryKeyNotFoundErrors.mandatoryKeyNotFoundError.message.format(ENABLE, "options.debug.enable"))
    isDebug = debugConfig.getBoolean(ENABLE)
  }

  private def parseHDFSConfig(hdfsConfig: Config): Unit = {

    if(!hdfsConfig.hasPath(ENABLE)) throw new MandatoryKeyNotFound(MandatoryKeyNotFoundErrors.mandatoryKeyNotFoundError.code,
      MandatoryKeyNotFoundErrors.mandatoryKeyNotFoundError.message.format(ENABLE, "options.hdfs.enable"))

    isHdfsEnabled = hdfsConfig.getBoolean(ENABLE)
    if(isHdfsEnabled) {
      if(!hdfsConfig.hasPath(PROPERTIES)) throw new MandatoryKeyNotFound(MandatoryKeyNotFoundErrors.mandatoryKeyNotFoundError.code,
        MandatoryKeyNotFoundErrors.mandatoryKeyNotFoundError.message.format(PROPERTIES, "options.hdfs.properties"))
      hdfsConfig.getObject(PROPERTIES).unwrapped.asScala.toMap.asInstanceOf[Map[String, String]].foreach(p => hdfsConfiguration.set(p._1, p._2))
    }
  }
}

private[sparkApp] object SparkAppConfig {
  private var builder: Option[SparkAppConfig] = None

  def instance: SparkAppConfig = builder.get

  private[sparkApp] def create(options: Config): SparkAppConfig = {
    if (builder.isEmpty) builder = Some(new SparkAppConfig(options))
    builder.get
  }
}
