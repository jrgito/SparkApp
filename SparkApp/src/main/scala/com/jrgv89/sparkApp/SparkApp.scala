package com.jrgv89.sparkApp

import java.io.File

import com.jrgv89.sparkApp.exceptions.CodeException
import com.jrgv89.sparkApp.ops.dfOps.DFOps
import com.jrgv89.sparkApp.table.{Table, TableBuilder}
import com.jrgv89.sparkApp.utils.Utils
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

abstract class SparkApp(conf: Config) extends Constants with DFOps with Loggeator {

  def this(configFile: String) {
    this(ConfigFactory.parseFile(new File(configFile)).resolve())
  }

  def this(configFile: String, sparkAppParentPath: String) {
    this(ConfigFactory.parseFile(new File(configFile)).resolve().getConfig(sparkAppParentPath))
  }

  def execute(spark: SparkSession): Int

  private val sparkAppConfig: Config = conf.getConfig(SPARK_APP)

  TableBuilder.create(sparkAppConfig.getConfig(DEFAULT_TABLE_PATH))

  private val name = sparkAppConfig.getString(SPARK_JOB_NAME_PATH)
  private val scMap = sparkAppConfig.getObject(SPARK_JOB_CONFIG_PATH).unwrapped.asScala.toMap.asInstanceOf[Map[String, String]]
  //  private val options = sparkAppConfig.getObject(OPTIONS_PATH).unwrapped.asScala.toMap.asInstanceOf[Map[String, String]]

  SparkAppConfig.create(sparkAppConfig.getConfig(OPTIONS_PATH))

  protected val tables: Map[String, Table] = TableBuilder.instance.tables(sparkAppConfig.getConfigList(TABLES_PATH).asScala)

  protected val appConfig: Config = sparkAppConfig.getConfig("app")

  /** **
    * Spark config
    * ***/

  private val sConf = new SparkConf().setAppName(name).setAll(scMap)
  protected lazy val spark: SparkSession = SparkSession.builder().config(sConf).getOrCreate()

  /**
    * method that start the process
    */

  def start(): Unit = {
    if (SparkAppConfig.instance.isDebug) {
      val (time, result) = Utils.time(execute(spark))
      println(s"time lapsed: " + time + " s")
      spark.stop
      System.exit(result.toString.toInt)
    } else {
      try {
        val (time, result) = Utils.time(execute(spark))
        println(s"time lapsed: " + time + " s")
        System.exit(result.toString.toInt)
      } catch {
        case e: CodeException =>
          spark.stop
          println(e)
          System.exit(e.getCode)
        case e: Exception =>
          spark.stop
          println(e)
          System.exit(500)
      }
    }
  }
}


