package com.jrgv89.spark.utils.dataframe

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

trait PopPartition extends Commons {

  val spark: SparkSession

  /**
    *
    * @param path
    * @param params
    * @return
    */
  private[dataframe] def load(path: Seq[String], partition: Seq[(String, Any)], params: Map[String, Any]): DataFrame = {

    val (format, options) = getParams(params)

    val dataFrame = spark
      .read
      .format(format)
      .options(options)
      .load(path: _*)

    partition.foldLeft(dataFrame)((df, p) => {
      df.filter(col(p._1) === p._2)
    })
  }

  /**
    *
    * @param params
    * @return
    */
  private def getParams(params: Map[String, Any]): (String, Map[String, String]) = {
    val format = params.getOrElse(FORMAT, spark.conf.get(DEFAULT_FORMAT)) match {
      case "jdbc" => throw new IllegalArgumentException("jdbc is not supported")
      case "avro" => AVRO_FORMAT
      case f: String => f
    }
    val options = if (params.contains(OPTIONS)) params.get(OPTIONS).asInstanceOf[Map[String, String]] else Map[String, String]()
    (format, options)
  }

  /**
    *
    * @param paths
    * @param format
    * @param options
    * @return
    */
  def getPartitions(paths: Seq[String], partition: Seq[(String, Any)], format: String = DEFAULT_FORMAT,
                    options: Map[String, String] = Map[String, String]()): DataFrame =
    load(paths, partition, Map(FORMAT -> format, OPTIONS -> options))

  /**
    *
    * @param paths
    * @param configuration
    */
  def deletePartitions(paths: Seq[String], partition: Seq[(String, Any)], configuration: Option[Configuration] = None): Unit = {
    val fs = FileSystem.get(configuration.getOrElse(new Configuration()))
    paths.foreach(path => {
      val partitionPath = new Path(path + createDeepPartition(partition))
      if (fs.exists(partitionPath)) {
        fs.delete(partitionPath, true)
      }
    })
  }
}
