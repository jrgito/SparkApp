package com.jrgv89.sparkApp.ops.tableOps

import com.jrgv89.sparkApp.exceptions.{PartitionNotFoundErrors, PartitionNotFoundException, TableOperationNotAllowedErrors, TableOperationNotAllowedException}
import com.jrgv89.sparkApp.ops.dfOps.DFUpdater
import com.jrgv89.sparkApp.table.Table
import com.jrgv89.sparkApp.utils.CheckDataFrame
import org.apache.spark.sql.DataFrame

/**
  * Created by JRGv89 on 19/05/2017.
  */
protected trait TableUpdater extends DFUpdater with TableReader {

  /**
    * update table with df
    *
    * @param table {Table}
    * @param df    {DataFrame}
    */
  protected def updateTable(df: DataFrame, table: Table): Unit = {

    if (!table.isReadOnly) {
      val schema = if (table.schema.isEmpty) {
        loggeator.info(s"Schema for table ${table.name} not set. Reading...")
        readTable(df.sparkSession, table, changeSchema = false).schema
      } else {
        table.schema.get
      }
      CheckDataFrame.areEquals(schema, df.schema)
      updateDF(df, table.inputPath + table.name, table.format, table.properties, table.partitionColumns)
    } else {
      throw new TableOperationNotAllowedException(TableOperationNotAllowedErrors.readOnlyTable.code,
        TableOperationNotAllowedErrors.readOnlyTable.message)
    }
  }

  /**
    * update table with df and timestamp
    *
    * @param table {Table}
    * @param df    {DataFrame}
    */
  protected def updateTableWithTS(df: DataFrame, table: Table, timestampColumn: String): Unit = {
    updateTable(withTS(df, timestampColumn), table)
  }


  /**
    * update a partition with df
    *
    * @param df             {DataFrame}
    * @param table          {Table}
    * @param partitionKey   {String}
    * @param partitionValue {String}
    */
  protected def updatePartition(df: DataFrame, table: Table, partitionKey: String, partitionValue: Any): Unit = {
    updateDeepPartition(df, table, Seq((partitionKey, partitionValue)))
  }

  /**
    * update a partition with df and timestamp
    *
    * @param df              {DataFrame}
    * @param table           {Table}
    * @param partitionKey    {String}
    * @param partitionValue  {String}
    * @param timestampColumn {String}
    */
  protected def updatePartitionWithTS(df: DataFrame, table: Table, partitionKey: String, partitionValue: Any, timestampColumn: String): Unit = {
    updateDeepPartition(withTS(df, timestampColumn), table, Seq((partitionKey, partitionValue)))
  }


  /**
    * update deep partition with df
    *
    * @param df         {DataFrame}
    * @param table      {Table}
    * @param partitions {Seq[(String, Any)]}
    */
  protected def updateDeepPartition(df: DataFrame, table: Table, partitions: Seq[(String, Any)]): Unit = {
    if (!table.isReadOnly) {
      val schema = if (table.schema.isEmpty) {
        loggeator.info(s"Schema for table ${table.name} not set. Reading...")
        readTable(df.sparkSession, table, changeSchema = false).schema
      } else {
        table.schema.get
      }
      CheckDataFrame.areEquals(schema, df.schema)
      table.partitionColumns.zip(partitions).foreach(p => if (!p._1.equals(p._2._1)) {
        throw new PartitionNotFoundException(PartitionNotFoundErrors.partitionNotFoundError.code,
          PartitionNotFoundErrors.partitionNotFoundError.message.format(table.name, p._2._1))
      })
      val partitionPath = createDeepPartition(partitions)
      updateDF(df, table.inputPath + table.name + partitionPath, table.format, table.properties)
    } else {
      throw new TableOperationNotAllowedException(TableOperationNotAllowedErrors.readOnlyTable.code,
        TableOperationNotAllowedErrors.readOnlyTable.message)
    }
  }

  /**
    * * update deep partition with df and timestamp
    *
    * @param df         {DataFrame}
    * @param table      {Table}
    * @param partitions {Seq[(String, Any)]}
    */
  protected def updateDeepPartitionWithTS(df: DataFrame, table: Table, partitions: Seq[(String, Any)], timestampColumn: String): Unit = {
    updateDeepPartition(withTS(df, timestampColumn), table, partitions)
  }


}
