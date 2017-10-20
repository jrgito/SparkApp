package com.jrgv89.dataframe.dfOps

import com.jrgv89.sparkApp.ops.Commons
import com.jrgv89.sparkApp.utils.Utils
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.Map

/**
  * Created by JRGv89 on 19/05/2017.
  */

trait DFReader extends Commons {

  /**
    * read file with options from path with spark
    *
    * @param spark   {SparkSession}
    * @param path    {String}
    * @param format  {String}
    * @param options {Map[String, String]}
    * @return {DataFrame}
    */
  def readDF(spark: SparkSession, path: String, format: String, options: Option[Map[String, String]] = None,
             partitions: Option[Seq[(String, Any)]] = None): DataFrame = {
    //TODO review
    val readerOptions = Utils.toMap(options.getOrElse(Map[String, String]()).getOrElse("readerOptions", Map[String, String]()))
    val df = format.toLowerCase match {
      case JSON | PARQUET => spark.read.format(format)
      case AVRO =>
        val defaults = Map[String, String]()
        spark.read.format(AVRO_FORMAT).options(defaults ++ readerOptions)
      case CSV =>
        val defaults: Map[String, String] = Map(headers, inferSchema)
        spark.read.format(CSV_FORMAT).options(defaults ++ readerOptions)
      case _ => spark.read
    }
    if (partitions.isEmpty) {
      df.load(path)
    } else {
      //TODO use foldLeft
      df.load(path).where(partitions.get.map(p => s"${p._1}=${transformPartitionValue(p._2)}").mkString(" and "))
    }
  }

  /**
    * read file with options from path with spark and register it with name
    *
    * @param spark   {SparkSession}
    * @param alias   {String}
    * @param path    {String}
    * @param format  {String}
    * @param options {Map[String, String]}
    * @return {DataFrame}
    */
  def readAndRegisterDF(spark: SparkSession, alias: String, path: String, format: String, options: Option[Map[String, String]] = None): DataFrame = {
    //TEST
    val df = readDF(spark, path, format, options).as(alias)
    //    df.registerTempTable(alias)
    df
  }


}
