package com.jrgv89.sparkApp.ops.tableOps

import com.jrgv89.sparkApp.Loggeator
import com.jrgv89.sparkApp.exceptions.{PartitionNotFoundErrors, PartitionNotFoundException}
import com.jrgv89.sparkApp.ops.dfOps.DFReader
import com.jrgv89.sparkApp.table.Table
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by JRGv89 on 19/05/2017.
  */

protected trait TableReader extends DFReader with Loggeator {

  /**
    * read table with spark
    *
    * @param spark {SparkSession}
    * @param table {Table}
    * @return {DataFrame}
    */
  protected def readTable(spark: SparkSession, table: Table, changeSchema: Boolean = true): DataFrame = {
    val df = readDF(spark, table.inputPath + table.name, table.format, table.properties)
    if (changeSchema) table.schema = Some(df.schema)
    df
  }

  /**
    * read partition with spark
    *
    * @param spark          {SparkSession}
    * @param table          {Table}
    * @param partitionKey   {String}
    * @param partitionValue {String}
    * @return {DataFrame}
    */
  protected def readPartition(spark: SparkSession, table: Table, partitionKey: String, partitionValue: Any): DataFrame = {
    readDeepPartition(spark, table, Seq((partitionKey, partitionValue)))
  }

  /**
    * read deep partition with spark
    *
    * @param spark      {SparkSession}
    * @param table      {Table}
    * @param partitions {Seq[(String, Any)]}
    * @return
    */
  protected def readDeepPartition(spark: SparkSession, table: Table, partitions: Seq[(String, Any)]): DataFrame = {
    table.partitionColumns.zip(partitions).foreach(p => if (!p._1.equals(p._2._1)) {
      throw new PartitionNotFoundException(PartitionNotFoundErrors.partitionNotFoundError.code,
        PartitionNotFoundErrors.partitionNotFoundError.message.format(table.name, p._2._1))
    })
    val df = readDF(spark, table.inputPath + table.name, table.format, table.properties, Some(partitions))
    table.schema = Some(df.schema)
    df
  }

  //TODO review and update this parts below
  /**
    * read table with spark and register its with name
    *
    * @param spark {SparkSession}
    * @param table {Table}
    * @param alias {Option[String]}
    * @return {DataFrame}
    */
  protected def readAndRegisterTable(spark: SparkSession, table: Table, alias: Option[String] = None): DataFrame = {
    val df = readTable(spark, table)
    val _alias = alias.getOrElse(table.name)
    if (alias.isDefined) {
      loggeator.info(s"table ${table.name} registered with name ${_alias}")
    }
    df.registerTempTable(_alias)
    df
  }

  /**
    * read table with spark and register its with name
    *
    * @param spark          {SparkSession}
    * @param table          {Table}
    * @param partitionKey   {String}
    * @param partitionValue {String}
    * @param alias          {String}
    * @return {DataFrame}
    */
  protected def readAndRegisterPartition(spark: SparkSession, table: Table, partitionKey: String, partitionValue: Any,
                                         alias: Option[String] = None): DataFrame = {
    readAndRegisterDeepPartition(spark, table, Seq((partitionKey, partitionValue)), alias)
  }

  /**
    *
    * @param spark      {SparkSession}
    * @param table      {Table}
    * @param partitions {Seq[(String, Any)]}
    * @param alias      {String}
    * @return
    */
  protected def readAndRegisterDeepPartition(spark: SparkSession, table: Table, partitions: Seq[(String, Any)],
                                             alias: Option[String] = None): DataFrame = {
    val df = readDeepPartition(spark, table, partitions)
    df.registerTempTable(alias.getOrElse(table.name))
    table.schema = Some(df.schema)
    df
  }


}
