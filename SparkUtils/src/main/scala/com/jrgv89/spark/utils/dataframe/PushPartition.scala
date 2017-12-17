package com.jrgv89.spark.utils.dataframe

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame

trait PushPartition extends Commons {

  val dataFrame: DataFrame

  /**
    *
    * @param path
    * @param params
    */
  private[dataframe] def createPartition(path: String, partition: Seq[(String, Any)], params: Map[String, Any]): Unit = {

    val (format, mode, options) = getParams(params)

    dataFrame
      .write
      .format(format)
      .mode(mode)
      .options(options)
      .save(path)

  }

  /**
    *
    * @param path
    * @param params
    * @param configuration
    */
  private[dataframe] def updatePartition(path: String, partition: Seq[(String, Any)], params: Map[String, Any], configuration: Configuration): Unit = {
    val fs = FileSystem.get(configuration)
    if (fs.exists(new Path(path))) {
      fs.delete(new Path(path), true)
    }
    val options = Map(PATH -> path, MODE -> "append") ++ params
    createPartition(path, partition, options)
  }

  /**
    *
    * @param params
    * @return
    */
  private def getParams(params: Map[String, Any]): (String, String, Map[String, String]) = {
    val format = params.getOrElse(FORMAT, dataFrame.sparkSession.conf.get(DEFAULT_FORMAT)) match {
      case "jdbc" => throw new IllegalArgumentException("jdbc is not supported")
      case "avro" => AVRO_FORMAT
      case f: String => f
    }
    val mode = params.getOrElse(MODE, DEFAULT_MODE).toString
    val options = if (params.contains(OPTIONS)) params.get(OPTIONS).asInstanceOf[Map[String, String]] else Map[String, String]()
    (format, mode, options)
  }

  /**
    *
    * @param path
    * @param format
    * @param mode
    * @param options
    */
  def createPartition(path: String, partition: Seq[(String, Any)], format: String = DEFAULT_FORMAT, mode: String = DEFAULT_MODE,
                      options: Map[String, String] = Map[String, String]()): Unit = {
    val params = Map(FORMAT -> format, MODE -> mode, OPTIONS -> options)
    createPartition(path, partition, params)
  }

  /**
    *
    * @param path
    * @param format
    * @param options
    * @param configuration
    */
  def updatePartition(path: String, partition: Seq[(String, Any)], format: String = DEFAULT_FORMAT, options: Map[String, String] = Map[String, String](),
                      configuration: Option[Configuration] = None): Unit = {
    val params = Map(FORMAT -> format, OPTIONS -> options)
    updatePartition(path, partition, params, configuration.getOrElse(new Configuration()))
  }


}
