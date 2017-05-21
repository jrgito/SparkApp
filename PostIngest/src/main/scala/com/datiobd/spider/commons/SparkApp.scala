package com.datiobd.spider.commons

import java.io.File

import com.datiobd.spider.commons.ops.dfOps.DFOps
import com.datiobd.spider.commons.exceptions.CodeException
import com.datiobd.spider.commons.table.{Table, TableBuilder}
import com.datiobd.spider.commons.utils.Utils
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._

abstract class SparkApp(conf: Config) extends Constants with DFOps {

  def this(configFile: String) {
    this(ConfigFactory.parseFile(new File(configFile)).resolve())
  }

  def execute(): Unit


  protected val appConf = conf.getConfig(CONFIG_APP)
  private val app = appConf.getConfig(CONFIG_CONFIG)
  SparkAppConfig.parseHDFSConfig(app.getConfig(CONFIG_HDFS))
  TableBuilder.instance(app.getConfig("defaultTable"))


  private val name = app.getString(NAME)
  private val scMap = app.getObject(CONFIG_SPARK).unwrapped.asScala.toMap.asInstanceOf[Map[String, String]]
  private val options = app.getObject(OPTIONS).unwrapped.asScala.toMap.asInstanceOf[Map[String, String]]
  protected val debug = options.getOrElse(DEBUG, false).toString.toBoolean
  val tables: Map[String, Table] = TableBuilder.create.tables(appConf.getConfigList("tables").asScala)
  /** **
    * Spark config
    * ***/

  private val sConf = new SparkConf().setAppName(name).setAll(scMap)
  private val sc = new SparkContext(sConf)
  val sqlContext = new SQLContext(sc)

  /**
    * method that start the process
    */
  def start(): Unit = {
    if (debug) {
      Utils.time("time lapsed => ", execute())
      sc.stop
      System.exit(0)
    } else {
      try {
        Utils.time("time lapsed => ", execute())
      } catch {
        case e: CodeException =>
          println(e)
          System.exit(e.getCode)
        case e: Exception =>
          println(e)
          System.exit(500)
      } finally {
        sc.stop
      }
    }
  }


}


