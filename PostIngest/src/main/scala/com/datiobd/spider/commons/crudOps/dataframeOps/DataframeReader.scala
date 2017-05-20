package com.datiobd.spider.commons.crudOps.dataframeOps

import com.datiobd.spider.commons.crudOps.Commons
import com.datiobd.spider.commons.utils.Utils
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.Map

/**
  * Created by JRGv89 on 19/05/2017.
  */

trait DataframeReader extends Commons{

  /**
    * read file with optios from path with sqlContext
    *
    * @param sqlContext {SQLContext}
    * @param path       {String}
    * @param format     {String}
    * @param options    {Map[String, String]}
    * @return {DataFrame}
    */
  def readDF(sqlContext: SQLContext, path: String, format: String, options: Option[Map[String, String]],
             partitions: Option[Seq[(String, Any)]] = None): DataFrame = {
    val readerOptions = Utils.toMap(options.getOrElse(Map[String, String]()).getOrElse("readerOptions", Map[String, String]()))
    val df = format.toLowerCase match {
      case JSON | PARQUET => sqlContext.read.format(format)
      case AVRO =>
        val defaults = Map[String, String]()
        sqlContext.read.format(AVRO_FORMAT).options(defaults ++ readerOptions)
      case CSV =>
        val defaults: Map[String, String] = Map(headers, inferSchema)
        sqlContext.read.format(CSV_FORMAT).options(defaults ++ readerOptions)
      case _ => sqlContext.read
    }
    if (partitions.isEmpty) {
      df.load(path)
    } else {
      //TODO use foldLeft
      df.load(path).where(partitions.get.map(p => s"${p._1}=${transformPartitionValue(p._2)}").mkString(" and "))
    }
  }

  /**
    * read file with options from path with sqlContext and register it with name
    *
    * @param sqlContext {SQLContext}
    * @param alias      {String}
    * @param path       {String}
    * @param format     {String}
    * @param options    {Map[String, String]}
    * @return {DataFrame}
    */
  def readAndRegisterDF(sqlContext: SQLContext, alias: String, path: String, format: String, options: Option[Map[String, String]] = None): DataFrame = {
    //TEST
    val df = readDF(sqlContext, path, format, options).as(alias)
//    df.registerTempTable(alias)
    df
  }


}
