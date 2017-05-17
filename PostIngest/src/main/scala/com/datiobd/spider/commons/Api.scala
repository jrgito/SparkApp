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
    val df = format match {
      case "json" | "parquet" => sqlContext.read.format(format)
      case "avro" =>
        val defaults = Map[String, String]()
        sqlContext.read.format(AVRO_FORMAT).options(defaults ++ readerOptions)
      case "csv" =>
        val defaults: Map[String, String] = Map(headers, inferSchema)
        sqlContext.read.format(CSV_FORMAT).options(defaults ++ readerOptions)
      case _ => sqlContext.read
    }
    if (partitions.isEmpty) {
      df.load(path)
    } else {
      df.load(path).where(partitions.get.map(p => s"${p._1}=${transformPartitionValue(p._2)}").mkString(" and "))
    }
  }

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
    val df = readDF(sqlContext, path, format, options)
    df.registerTempTable(alias)
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
    * write df with table properties
    *
    * @param df    {DataFrame}
    * @param table {Table}
    */
  @deprecated
  def writePartition(df: DataFrame, table: Table, partition: String): Unit = {
    writeDF(df, table.path + table.name + partition, table.format, table.writeMode, table.properties, Seq())
  }

  /**
    *
    * @param df             {DataFrame}
    * @param table          {Table}
    * @param partitionKey   {String}
    * @param partitionValue {String}
    */
  def writePartition(df: DataFrame, table: Table, partitionKey: String, partitionValue: Any): Unit = {
    writeDF(df, table.path + table.name + createPartition(partitionKey, partitionValue), table.format, table.writeMode, table.properties, Seq())
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
    * delete a table directory
    *
    * @param table {Table} table to delete
    */
  def deleteTable(table: Table): Unit = {
    deleteDirectory(table.path + table.name)
  }

  /**
    *
    * delete a tables directory
    *
    * @param table          {Table} table to delete
    * @param partitionKey   {String} partition key
    * @param partitionValue {String} partition value
    */
  def deletePartition(table: Table, partitionKey: String, partitionValue: Any): Unit = {
    deleteDeepPartition(table, Seq((partitionKey, partitionValue)))
  }


  def deleteDeepPartition(table: Table, partitions: Seq[(String, Any)]): Unit = {
    deleteDirectory(table.path + table.name + createDeepPartition(partitions))
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
