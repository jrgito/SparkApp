package com.datiobd.spider.commons.crudOps.dataframeOps

import com.datiobd.spider.commons.Utils
import com.datiobd.spider.commons.crudOps.Commons
import com.datiobd.spider.commons.table.Table
import org.apache.spark.sql.{DataFrame, DataFrameWriter}

import scala.collection.Map

/**
  * Created by JRGv89 on 19/05/2017.
  */
trait DataframeWriter extends Commons {

  /**
    * writeDF
    *
    * @param df      {DataFrame}
    * @param path    {String}
    * @param format  {String}
    * @param mode    {String}
    * @param options {Map[String, String]}
    */
  def writeDF(df: DataFrame, path: String, format: String, mode: String, options: Option[Map[String, String]] = None): Unit = {
    writeDF(df, path, format, mode, options, Seq[String]())
  }

  /**
    *
    * @param df               {DataFrame}
    * @param path             {String}
    * @param format           {String}
    * @param mode             {String}
    * @param partitionColumns {Seq[String]}
    */
  def writeDF(df: DataFrame, path: String, format: String, mode: String, partitionColumns: Seq[String]): Unit = {
    writeDF(df, path, format, mode, None, partitionColumns)

  }


  /**
    * write df with options
    *
    * @param df               {DataFrame}
    * @param path             {String}
    * @param format           {String}
    * @param mode             {String}
    * @param options          {Map[String, String]}
    * @param partitionColumns {Seq[String]}
    */
  def writeDF(df: DataFrame, path: String, format: String, mode: String, options: Option[Map[String, String]], partitionColumns: Seq[String]): Unit = {
    val writerOptions = Utils.toMap(options.getOrElse(Map[String, String]()).getOrElse("writerOptions", Map[String, String]()))
    val dfw: DataFrameWriter = (format match {
      case "parquet" | "json" => df.write.format(format)
      case "avro" =>
        val defaults = Map[String, String]()
        df.write.format(AVRO_FORMAT).options(defaults ++ writerOptions)
      case "csv" =>
        val defaults = Map("header" -> "true")
        df.write.format(CSV_FORMAT).options(defaults ++ writerOptions)
      case _ => df.write.format(format)
    }).mode(mode)
    if (partitionColumns.isEmpty) {
      dfw.save(path)
    } else {
      dfw.partitionBy(partitionColumns: _*).save(path)
    }
  }

}
