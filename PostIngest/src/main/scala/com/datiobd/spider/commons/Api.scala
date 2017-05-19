package com.datiobd.spider.commons

import java.sql.Date
import java.util.Calendar

import com.datiobd.spider.commons.utils.FileOps
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
