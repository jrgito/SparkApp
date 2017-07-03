package com.datiobd.spider.commons

import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration

import scala.collection.JavaConverters._

/**
  * Created by JRGv89 on 20/05/2017.
  */
private[commons] object SparkAppConfig {
  val PROPERTIES = "properties"
  val ENABLE = "enable"
  val DEBUG = "debug"
  var debug = true
  var isHdfsEnable = true
  var hdfsConfig: Configuration = new Configuration()

  def isDebug: Boolean = debug

   def parseHDFSConfig(config: Config): Unit = {

    if (config.hasPath(ENABLE) && config.getBoolean(ENABLE)) {
      val _hdfsConfig = new Configuration()
      config.getObject(PROPERTIES).unwrapped.asScala.toMap.asInstanceOf[Map[String, String]].foreach(p => _hdfsConfig.set(p._1, p._2))
      isHdfsEnable = true
      hdfsConfig = _hdfsConfig
    } else {
      isHdfsEnable = false
      hdfsConfig = new Configuration()
    }
  }
}
