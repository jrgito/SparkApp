package com.datiobd.spider.commons.crudOps

import java.sql.Date
import java.util.Calendar

/**
  * Created by JRGv89 on 19/05/2017.
  */
protected trait Commons {

  val JSON = "json"
  val CSV = "csv"
  val PARQUET = "parquet"
  val AVRO = "avro"

  val TRUE: String = "true"
  val TEMP_DIR_PATH: String = "tmp/"
  val MODE: String = "overwrite"
  val SINGLE_QUOTE: String = "'"


  val headers: (String, String) = ("header", TRUE)
  val inferSchema: (String, String) = ("inferSchema", TRUE)

  val CSV_FORMAT: String = "com.databricks.spark.csv"
  val AVRO_FORMAT: String = "com.databricks.spark.avro"

  def idDebug(): Boolean


  /**
    * creates a good partition
    *
    * @param key  {String} partition key
    * @param data {String} partition value
    * @return
    */
  def createPartition(key: String, data: Any): String = data match {
    case _: java.sql.Date => s"/$key=$data"
    case _: java.util.Date => s"/$key=${new Date(data.asInstanceOf[java.util.Date].getTime)}"
    case _: Calendar => s"/$key=${new Date(data.asInstanceOf[Calendar].getTimeInMillis)}"
    case _ => s"/$key=$data"
  }

  def createDeepPartition(partitions: Seq[(String, Any)]): String = partitions.map(p => createPartition(p._1, p._2)).mkString

  /**
    * transform data for where clause
    *
    * @param data {Any} any value
    * @return {Any}
    */
  def transformPartitionValue(data: Any): Any = data match {
    case _: java.sql.Date => SINGLE_QUOTE + data + SINGLE_QUOTE
    case _: java.util.Date => SINGLE_QUOTE + new Date(data.asInstanceOf[java.util.Date].getTime) + SINGLE_QUOTE
    case _: Calendar => SINGLE_QUOTE + new Date(data.asInstanceOf[Calendar].getTimeInMillis) + SINGLE_QUOTE
    case _ => data
  }

}
