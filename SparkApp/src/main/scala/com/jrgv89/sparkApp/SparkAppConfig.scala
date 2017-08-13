package com.jrgv89.sparkApp

import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration

import scala.collection.JavaConverters._

/**
  * Created by JRGv89 on 20/05/2017.
  */
private[sparkApp] class SparkAppConfig(val isDebug: Boolean) extends Constants {

  var isHdfsEnable = false
  var hdfsConfig: Configuration = new Configuration()


  private[sparkApp] def parseHDFSConfig(sparkAppConfig: Config): Unit = {

    if (sparkAppConfig.hasPath(HDFS_ENABLE_PATH) && sparkAppConfig.getBoolean(HDFS_ENABLE_PATH)) {
      val _hdfsConfig = new Configuration()
      sparkAppConfig.getObject(HDFS_PROPERTIES_PATH).unwrapped.asScala.toMap.asInstanceOf[Map[String, String]].foreach(p => _hdfsConfig.set(p._1, p._2))
      isHdfsEnable = true
      hdfsConfig = _hdfsConfig
    } else {
      isHdfsEnable = false
      hdfsConfig = new Configuration()
    }
  }
}

private[sparkApp] object SparkAppConfig {
  private var builder: Option[SparkAppConfig] = None

  def instance: SparkAppConfig = builder.get

  private[sparkApp] def create(debug: Boolean): SparkAppConfig = {
    if (builder.isEmpty) builder = Some(new SparkAppConfig(debug))
    builder.get
  }
}
