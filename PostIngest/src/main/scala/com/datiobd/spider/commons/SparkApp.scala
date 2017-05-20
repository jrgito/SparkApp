package com.datiobd.spider.commons

import java.io.File

import com.datiobd.spider.commons.table.{Table, TableBuilder}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

abstract class SparkApp(conf: Config) extends Api with SinfoLogger with Serializable with TimeUtils with SQLUtils with Constants {

  def this(configFile: String) {
    this(ConfigFactory.parseFile(new File(configFile)).resolve())
  }

  override def isDebug: Boolean = debug

  override def isHDFSEnable: Boolean = hdfs

  override def getHDFSConfig: Configuration = hdfsConfig

  override def getTables: Map[String, Table] = tables

  def execute(): Unit

  def parseHDFSConfig(config: Config): (Configuration, Boolean) = {
    val _HDFSConfig = config.getConfig(CONFIG_HDFS)
    if (_HDFSConfig.hasPath(ENABLE) && _HDFSConfig.getBoolean(ENABLE)) {
      val _hdfsConfig = new Configuration()
      _HDFSConfig.getObject(PROPERTIES).unwrapped.asScala.toMap.asInstanceOf[Map[String, String]].foreach(p => _hdfsConfig.set(p._1, p._2))
      (_hdfsConfig, true)
    } else {
      (new Configuration(), false)
    }
  }

  protected val appConf = conf.getConfig(CONFIG_APP)
  private val app = appConf.getConfig(CONFIG_CONFIG)
  private val name = app.getString(NAME)
  private val scMap = app.getObject(CONFIG_SPARK).unwrapped.asScala.toMap.asInstanceOf[Map[String, String]]
  private val options = app.getObject(OPTIONS).unwrapped.asScala.toMap.asInstanceOf[Map[String, String]]
  private val (hdfsConfig, hdfs) = parseHDFSConfig(app)
  val logger = LoggerFactory.getLogger(name)
  protected val debug = options.getOrElse(DEBUG, false).toString.toBoolean
  TableBuilder.instance(app.getConfig("defaultTable"))
  val tables: Map[String, Table] = TableBuilder.instance()
    .createTables(appConf.getConfigList("tables").asScala)
  /** **
    * Spark config
    * ***/

  val sConf = new SparkConf().setAppName(name).setAll(scMap)
  val sc = new SparkContext(sConf)
  val sqlContext = new SQLContext(sc)
  //   val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
  /**
    * method that start the process
    */
  def start(): Unit = {
    if (debug) {
      time("time lapsed => ", execute())
      sc.stop
    } else {
      try {
        time("time lapsed => ", execute())
      } catch {
        case e: Exception =>
          println("Exception caugth")
          println(e)
      } finally {
        sc.stop
      }
    }
  }

  // import sqlContext.implicits._ should be imported because is used in the child classes.


  /**
    * log msg in shell
    *
    * @param msg {String} msg
    */
  @deprecated
  def printLog(msg: Any): Unit = {
    if (debug) {
      log.error("MASTER:: " + msg.toString)
      logError("EXEC:: " + msg.toString)
    }
  }

  /**
    * show df data
    *
    * @param name {String} name
    * @param df   {DataFrame} df
    */
  @deprecated("use sLog")
  def printDF(name: String, df: DataFrame): Unit = {
    if (debug) {
      println(name)
      log.error("MASTER:: " + name)
      logError("EXEC:: " + name)
      df.show()
    }
  }
}


