package com.datiobd.spider.commons.crudOps.tableOps

import com.datiobd.spider.commons.{SinfoLogLevel, Utils}
import com.datiobd.spider.commons.crudOps.Commons
import com.datiobd.spider.commons.crudOps.dataframeOps.DataframeReader
import com.datiobd.spider.commons.table.Table
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.Map

/**
  * Created by JRGv89 on 19/05/2017.
  */

protected trait TableReader extends DataframeReader{



  /**
    * read table with sqlContext
    *
    * @param sqlContext {SQLContext}
    * @param table      {Table}
    * @return {DataFrame}
    */
  def readTable(sqlContext: SQLContext, table: Table): DataFrame = {
    val df = readDF(sqlContext, table.path + table.name, table.format, table.properties)
    table.schema = Some(df.schema)
    df
  }

  /**
    * read table with sqlContext and register its with name
    *
    * @param sqlContext {SQLContext}
    * @param table      {Table}
    * @param alias      {Option[String]}
    * @return {DataFrame}
    */
  def readAndRegisterTable(sqlContext: SQLContext, table: Table, alias: Option[String] = None): DataFrame = {
    val df = readTable(sqlContext, table)
    val _alias = alias.getOrElse(table.name)
    if (alias.isDefined) {
      sLog(SinfoLogLevel.INFO, s"table ${table.name} registered with name ${_alias}")
    }
    df.registerTempTable(_alias)
    df
  }

  /**
    * read partition with sqlContext
    *
    * @param sqlContext     {SQLContext}
    * @param table          {Table}
    * @param partitionKey   {String}
    * @param partitionValue {String}
    * @return {DataFrame}
    */
  def readPartition(sqlContext: SQLContext, table: Table, partitionKey: String, partitionValue: Any): DataFrame = {
    readDeepPartition(sqlContext, table, Seq((partitionKey, partitionValue)))
  }

  def readDeepPartition(sqlContext: SQLContext, table: Table, partitions: Seq[(String, Any)]): DataFrame = {
    table.partitionColumns.zip(partitions).foreach(p => if (!p._1.equals(p._2._1)) {
      throw new Exception(s"table ${table.name} has not partition column ${p._2._1}")
    })
    val df = readDF(sqlContext, table.path + table.name, table.format, table.properties, Some(partitions))
    table.schema = Some(df.schema)
    df
  }

  /**
    * read table with sqlContext and register its with name
    *
    * @param sqlContext     {SQLContext}
    * @param table          {Table}
    * @param partitionKey   {String}
    * @param partitionValue {String}
    * @param alias          {String}
    * @return {DataFrame}
    */
  def readAndRegisterPartition(sqlContext: SQLContext, table: Table, partitionKey: String, partitionValue: Any, alias: Option[String] = None): DataFrame = {
    readAndRegisterDeepPartition(sqlContext, table, Seq((partitionKey, partitionValue)), alias)
  }

  def readAndRegisterDeepPartition(sqlContext: SQLContext, table: Table, partitions: Seq[(String, Any)], alias: Option[String] = None): DataFrame = {
    table.partitionColumns.zip(partitions).foreach(p => if (!p._1.equals(p._2._1)) {
      throw new Exception(s"table ${table.name} has not partition column ${p._2._1}")
    })
    val df = readDF(sqlContext, table.path + table.name, table.format, table.properties, Some(partitions))
    df.registerTempTable(alias.getOrElse(table.name))
    table.schema = Some(df.schema)
    df
  }


}
