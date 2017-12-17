package com.jrgv89.spark.utils.dataframe

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}

trait Pop extends Commons {

  val spark: SparkSession

  /**
    *
    * @param path
    * @param params
    * @return
    */
  private[dataframe] def load(path: Seq[String], params: Map[String, Any]): DataFrame = {

    val (format, options) = getParams(params)

    spark
      .read
      .format(format)
      .options(options)
      .load(path: _*)

  }

  /**
    *
    * @param params
    * @return
    */
  private def getParams(params: Map[String, Any]): (String, Map[String, String]) = {
    val format = params.getOrElse(FORMAT, spark.conf.getOption(DEFAULT_FORMAT).getOrElse(DEFAULT_FORMAT)) match {
      case "jdbc" => throw new IllegalArgumentException("jdbc is not supported")
      case "avro" => AVRO_FORMAT
      case f: String => f
    }
    val options = if (params.contains(OPTIONS)) params(OPTIONS).asInstanceOf[Map[String, String]] else Map[String, String]()
    (format, options)
  }

  /**
    *
    * @param paths
    * @param format
    * @param options
    * @return
    */
  def get(paths: Seq[String], format: String = DEFAULT_FORMAT,
          options: Map[String, String] = Map[String, String]()): DataFrame =
    load(paths, Map(FORMAT -> format, OPTIONS -> options))

  /**
    *
    * @param paths
    * @param configuration
    */
  def delete(paths: Seq[String], configuration: Option[Configuration] = None): Unit = {
    val fs = FileSystem.get(configuration.getOrElse(new Configuration()))
    paths.foreach(path => fs.delete(new Path(path), true))
  }
}
