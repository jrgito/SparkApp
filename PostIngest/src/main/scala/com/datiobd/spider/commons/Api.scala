package com.datiobd.spider.commons

import java.sql.Date
import java.util.Calendar

import org.apache.spark.sql.{DataFrame, DataFrameWriter, SQLContext}
import scala.collection.Map

abstract class Api extends FileOps with SinfoLogger {

  def isDebug: Boolean
  def getTables : Map[String, Table]

  val TRUE = "true"
  val CSV_FORMAT = "com.databricks.spark.csv"
  val AVRO_FORMAT = "com.databricks.spark.avro"
  val headers = ("header", TRUE)
  val inferSchema = ("inferSchema", TRUE)
  val tempDirectoryPath = "tmp/"
  val mode = "overwrite"
  val SINGLE_QUOTE = "'"


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


  /**
    * register df as name tempTable
    *
    * @param df    {DataFrame}
    * @param alias {String}
    * @return DataFrame
    */
  def registerDF(df: DataFrame, alias: String): DataFrame = {
    df.registerTempTable(alias)
    df
  }

  /**
    * write df with table properties
    *
    * @param df    {DataFrame}
    * @param table {Table}
    */
  def writeTable(df: DataFrame, table: Table): Unit = {
    writeDF(df, table.path + table.name, table.format, table.writeMode, table.properties, table.partitionColumns)
  }

  /**
    * write df with table properties
    *
    * @param table {Table}
    * @param df    {DataFrame}
    */
  def updateTable(df: DataFrame, table: Table): Unit = {
    val schema = if (table.schema.isEmpty) {
      //TODO ADD slog
      println(s"Schema for table ${table.name} not set. Reading...")
      readTable(df.sqlContext, table).schema
    } else {
      table.schema.get
    }
    Utils.areEqual(schema, df.schema)
    writeDF(df, table.path + tempDirectoryPath + table.name, table.format, table.writeMode, table.properties, table.partitionColumns)
    moveDirectory(table.path + tempDirectoryPath + table.name, table.path + table.name)

  }




  /**
    * update a partition
    *
    * @param df             {DataFrame}
    * @param table          {Table}
    * @param partitionKey   {String}
    * @param partitionValue {String}
    */
  def updatePartition(df: DataFrame, table: Table, partitionKey: String, partitionValue: Any): Unit = {
    updateDeepPartition(df, table, Seq((partitionKey, partitionValue)))
  }

  def updateDeepPartition(df: DataFrame, table: Table, partitions: Seq[(String, Any)]): Unit = {
    val schema = if (table.schema.isEmpty) {
      println(s"Schema for table ${table.name} not set. Reading...")
      readTable(df.sqlContext, table).schema
    } else {
      table.schema.get
    }
    Utils.areEqual(schema, df.schema)
    table.partitionColumns.zip(partitions).foreach(p => if (!p._1.equals(p._2._1)) {
      throw new Exception(s"table ${table.name} has not partition column ${p._2._1}")
    })
    val partitionPath = createDeepPartition(partitions)
    writeDF(df, table.path + tempDirectoryPath + table.name, table.format, table.writeMode, table.properties, Seq())
    moveDirectory(table.path + tempDirectoryPath + table.name, table.path + table.name + partitionPath)
  }








  /**
    * show table if is in debug mode
    *
    * @param sqlContext {SQLContext}
    * @param table      {Table} table to debug
    */
  def debugTable(sqlContext: SQLContext, table: Table): Unit = {
    if (isDebug) {
      println("**************")
      println(s"* DEBUG: ${table.name}")
      println("**************")
      readTable(sqlContext, table).show
    }
  }

  def getTableDataframeGroup(sqlContext: SQLContext,tableName :String) : (Table, DataFrame) ={
    val table =  getTables(tableName)
    val df = readTable(sqlContext, table)
    (table, df)
  }

  def getTableDataframeGroupPartition(sqlContext: SQLContext,tableName :String, partition: Any) : (Table, DataFrame) ={
    val table =  getTables(tableName)
    val df = readAndRegisterPartition(sqlContext, table, table.partitionColumns.head, partition)
    (table, df)
  }

}
