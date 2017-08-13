package com.jrgv89.sparkApp.ops.tableOps

import com.jrgv89.sparkApp.exceptions._
import com.jrgv89.sparkApp.ops.dfOps.DFWriter
import com.jrgv89.sparkApp.table.Table
import org.apache.spark.sql.DataFrame

/**
  * Created by JRGv89 on 19/05/2017.
  */
trait TableWriter extends DFWriter {
  /**
    * write df with table properties
    *
    * @param df    {DataFrame}
    * @param table {Table}
    */
  def writeTable(df: DataFrame, table: Table): Unit = {
    if (!table.isReadOnly) {
      writeDF(df, table.outputPath + table.name, table.format, table.writeMode, table.properties, table.partitionColumns)
    } else {
      throw new TableOperationNotAllowedException(TableOperationNotAllowedErrors.readOnlyTable.code,
        TableOperationNotAllowedErrors.readOnlyTable.message)
    }
  }

  /**
    * write df with table properties
    *
    * @param df    {DataFrame}
    * @param table {Table}
    */
  def writeTableWithTS(df: DataFrame, table: Table, timestampColumn: String): Unit = {
    if (!table.isReadOnly) {
      writeTable(withTS(df, timestampColumn), table)
    } else {
      throw new TableOperationNotAllowedException(TableOperationNotAllowedErrors.readOnlyTable.code,
        TableOperationNotAllowedErrors.readOnlyTable.message)
    }
  }

  /**
    *
    * @param df             {DataFrame}
    * @param table          {Table}
    * @param partitionKey   {String}
    * @param partitionValue {String}
    */
  def writePartition(df: DataFrame, table: Table, partitionKey: String, partitionValue: Any): Unit = {
    if (!table.isReadOnly) {
      writeDF(df, table.outputPath + table.name + createPartition(partitionKey, partitionValue), table.format, table.writeMode, table.properties, Seq())
    } else {
      throw new TableOperationNotAllowedException(TableOperationNotAllowedErrors.readOnlyTable.code,
        TableOperationNotAllowedErrors.readOnlyTable.message)
    }
  }

  /**
    *
    * @param df             {DataFrame}
    * @param table          {Table}
    * @param partitionKey   {String}
    * @param partitionValue {String}
    */
  def writePartitionWithTS(df: DataFrame, table: Table, partitionKey: String, partitionValue: Any, timestampColumn: String): Unit = {
    if (!table.isReadOnly) {
      writePartition(withTS(df, timestampColumn), table, partitionKey, partitionValue)
    } else {
      throw new TableOperationNotAllowedException(TableOperationNotAllowedErrors.readOnlyTable.code,
        TableOperationNotAllowedErrors.readOnlyTable.message)
    }
  }

  /**
    *
    * @param df         {DataFrame}
    * @param table      {Table}
    * @param partitions {Seq[(String, Any)]}
    */
  def writeDeepPartition(df: DataFrame, table: Table, partitions: Seq[(String, Any)]): Unit = {
    if (!table.isReadOnly) {
      table.partitionColumns.zip(partitions).foreach(p => if (!p._1.equals(p._2._1)) {
        throw new PartitionNotFoundException(PartitionNotFoundErrors.partitionNotFoundError.code,
          PartitionNotFoundErrors.partitionNotFoundError.message.format(table.name, p._2._1))
      })
      writeDF(df, table.outputPath + table.name + createDeepPartition(partitions), table.format, table.writeMode, table.properties, Seq())
    } else {
      throw new TableOperationNotAllowedException(TableOperationNotAllowedErrors.readOnlyTable.code,
        TableOperationNotAllowedErrors.readOnlyTable.message)
    }
  }


  /**
    *
    * @param df         {DataFrame}
    * @param table      {Table}
    * @param partitions {Seq[(String, Any)]}
    */
  def writeDeepPartitionWithTS(df: DataFrame, table: Table, partitions: Seq[(String, Any)], timestampColumn: String): Unit = {
    if (!table.isReadOnly) {
      writeDeepPartition(withTS(df, timestampColumn), table, partitions)
    } else {
      throw new TableOperationNotAllowedException(TableOperationNotAllowedErrors.readOnlyTable.code,
        TableOperationNotAllowedErrors.readOnlyTable.message)
    }
  }


}
