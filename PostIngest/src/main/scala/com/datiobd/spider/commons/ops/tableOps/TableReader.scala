package com.datiobd.spider.commons.ops.tableOps

import com.datiobd.spider.commons.SinfoLogLevel
import com.datiobd.spider.commons.ops.dfOps.DFReader
import com.datiobd.spider.commons.exceptions.{PartitionNotFoundErrors, PartitionNotFoundException}
import com.datiobd.spider.commons.table.Table
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by JRGv89 on 19/05/2017.
  */

protected trait TableReader extends DFReader {


  /**
    * read table with sqlContext
    *
    * @param sqlContext {SQLContext}
    * @param table      {Table}
    * @return {DataFrame}
    */
  def readTable(sqlContext: SQLContext, table: Table, changeSchema: Boolean = true): DataFrame = {
    val df = readDF(sqlContext, table.path + table.name, table.format, table.properties)
    if (changeSchema) table.schema = Some(df.schema)
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

  /**
    *
    * @param sqlContext {SQLContext}
    * @param table      {Table}
    * @param partitions {Seq[(String, Any)]}
    * @return
    */
  def readDeepPartition(sqlContext: SQLContext, table: Table, partitions: Seq[(String, Any)]): DataFrame = {
    table.partitionColumns.zip(partitions).foreach(p => if (!p._1.equals(p._2._1)) {
      throw new PartitionNotFoundException(PartitionNotFoundErrors.partitionNotFoundError.code,
        PartitionNotFoundErrors.partitionNotFoundError.message.format(table.name, p._2._1))
    })
    val df = readDF(sqlContext, table.path + table.name, table.format, table.properties, Some(partitions))
    table.schema = Some(df.schema)
    df
  }

  //TODO update this parts below
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
      //TODO
//      sLog(SinfoLogLevel.INFO, s"table ${table.name} registered with name ${_alias}")
    }
    df.registerTempTable(_alias)
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

  /**
    *
    * @param sqlContext {SQLContext}
    * @param table      {Table}
    * @param partitions {Seq[(String, Any)]}
    * @param alias      {String}
    * @return
    */
  def readAndRegisterDeepPartition(sqlContext: SQLContext, table: Table, partitions: Seq[(String, Any)], alias: Option[String] = None): DataFrame = {
    val df = readDeepPartition(sqlContext, table,  partitions)
    df.registerTempTable(alias.getOrElse(table.name))
    table.schema = Some(df.schema)
    df
  }


}
