package com.jrgv89.spark.utils.dataframe


import java.sql.Timestamp
import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

trait Push extends Commons {

  val dataFrame: DataFrame

  /**
    *
    * @param path
    * @param params
    */
  private[dataframe] def create(path: String, params: Map[String, Any]): Unit = {

    val (format, mode, partitionBy, options) = getParams(params)

    dataFrame
      .write
      .format(format)
      .mode(mode)
      .partitionBy(partitionBy: _*)
      .options(options)
      .save(path)

  }

  /**
    *
    * @param path
    * @param params
    * @param configuration
    */
  private[dataframe] def update(path: String, params: Map[String, Any], configuration: Configuration): Unit = {
    val fs = FileSystem.get(configuration)
    if (fs.exists(new Path(path))){
      fs.delete(new Path(path), true)
    }
    val options = Map(PATH -> path, MODE -> "append") ++ params
    create(path, options)
  }

  /**
    *
    * @param params
    * @return
    */
  private def getParams(params: Map[String, Any]): (String, String, Seq[String], Map[String, String]) = {
    val format = params.getOrElse(FORMAT, dataFrame.sparkSession.conf.get(DEFAULT_FORMAT)) match {
      case "jdbc" => throw new IllegalArgumentException("jdbc is not supported")
      case "avro" => AVRO_FORMAT
      case f: String => f
    }
    val mode = params.getOrElse(MODE, DEFAULT_MODE).toString
    val partitionBy: Seq[String] = if (params.contains(PARTITION_BY)) params.get(PARTITION_BY).asInstanceOf[Seq[String]] else Seq[String]()
    val options = if (params.contains(OPTIONS)) params.get(OPTIONS).asInstanceOf[Map[String, String]] else Map[String, String]()
    (format, mode, partitionBy, options)
  }

  /**
    *
    * @param path
    * @param format
    * @param mode
    * @param options
    * @param partitionBy
    */
  def create(path: String, format: String = DEFAULT_FORMAT, mode: String = DEFAULT_MODE,
             options: Map[String, String] = Map[String, String](), partitionBy: Seq[String] = Seq[String]()): Unit = {
    val params = Map(FORMAT -> format, MODE -> mode, OPTIONS -> options, PARTITION_BY -> partitionBy)
    create(path, params)
  }

  /**
    *
    * @param path
    * @param format
    * @param options
    * @param partitionBy
    * @param configuration
    */
  def update(path: String, format: String = DEFAULT_FORMAT, options: Map[String, String] = Map[String, String](),
             partitionBy: Seq[String] = Seq[String](), configuration: Option[Configuration] = None): Unit = {
    val params = Map(FORMAT -> format, OPTIONS -> options, PARTITION_BY -> partitionBy)
    update(path, params, configuration.getOrElse(new Configuration()))
  }


}
