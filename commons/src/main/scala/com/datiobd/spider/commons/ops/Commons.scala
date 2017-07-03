package com.datiobd.spider.commons.ops

import java.sql.{Date, Timestamp}
import java.util.Calendar

import com.datiobd.spider.commons.SparkAppConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

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


  val partitionPath = s"/%s=%s"

  protected def isDebug: Boolean = SparkAppConfig.debug

  //TODO refact??

  /**
    * creates a good partition
    *
    * @param key  {String} partition key
    * @param data {String} partition value
    * @return
    */
  def createPartition(key: String, data: Any): String = data match {
    case _: java.sql.Date => partitionPath.format(key, data)
    case _: java.util.Date => partitionPath.format(key, new Date(data.asInstanceOf[java.util.Date].getTime))
    case _: Calendar => partitionPath.format(key, new Date(data.asInstanceOf[Calendar].getTimeInMillis))
    case _ => partitionPath.format(key, data)
  }

  /**
    *
    * @param partitions {Seq[(String, Any)]}
    * @return
    */
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

  /**
    * returns a dataFrame with timestamp
    *
    * @param df     {DataFrame}
    * @param column {Column}
    * @return dataframe with timestamp column
    */
  def withTS(df: DataFrame, column: String): DataFrame = df.withColumn(column, lit(new Timestamp(Calendar.getInstance().getTimeInMillis)))

}
