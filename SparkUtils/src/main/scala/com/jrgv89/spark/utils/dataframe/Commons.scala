package com.jrgv89.spark.utils.dataframe

import java.sql.Date
import java.util.Calendar

protected[dataframe] trait Commons {
  val MODE = "mode"
  val DEFAULT_MODE = "default"
  val FORMAT = "format"
  val DEFAULT_FORMAT = "parquet"
  val AVRO_FORMAT: String = "com.databricks.spark.avro"
  val OPTIONS = "options"
  val PARTITION_BY = "partitionBy"
  val PATH = "path"
  val DEFAULT_SOURCE = "spark.sql.sources.default"


  val partitionPath = s"/%s=%s"

  /**
    * creates a good partition
    *
    * @param key  {String} partition key
    * @param data {String} partition value
    * @return
    */
  protected[dataframe] def createPartition(key: String, data: Any): String = data match {
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
  protected[dataframe] def createDeepPartition(partitions: Seq[(String, Any)]): String = partitions.map(p => createPartition(p._1, p._2)).mkString


}
