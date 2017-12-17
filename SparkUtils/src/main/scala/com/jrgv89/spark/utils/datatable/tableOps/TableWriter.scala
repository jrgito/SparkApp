package com.jrgv89.spark.utils.datatable.tableOps

import com.jrgv89.spark.utils.dataframe.Implicits._
import com.jrgv89.spark.utils.datatable.DataTable
import com.jrgv89.spark.utils.datatable.exceptions.{PartitionNotFoundErrors, PartitionNotFoundException, TableOperationNotAllowedErrors, TableOperationNotAllowedException}
import com.jrgv89.spark.utils.utils.Utils._
import org.apache.spark.sql.DataFrame

/** i
  * Created by JRGv89 on 19/05/2017.
  */
trait TableWriter {

  /**
    * write table in a specific path
    *
    * @param df    {DataFrame}
    * @param table {DataTable}
    */
  protected def writeTable(df: DataFrame, table: DataTable): Unit = {
    if (!table.isReadOnly) {
      df.create(table.outputPath + table.name, table.format, table.writeMode, table.properties, table.partitionColumns)
    } else {
      throw new TableOperationNotAllowedException(TableOperationNotAllowedErrors.readOnlyTable.code,
        TableOperationNotAllowedErrors.readOnlyTable.message)
    }
  }

  /**
    * write table in a specific path with timestamp
    *
    * @param df    {DataFrame}
    * @param table {DataTable}
    */
  protected def writeTableWithTS(df: DataFrame, table: DataTable, timestampColumn: String): Unit = {
    if (!table.isReadOnly) {
      writeTable(df.withTS(timestampColumn), table)
    } else {
      throw new TableOperationNotAllowedException(TableOperationNotAllowedErrors.readOnlyTable.code,
        TableOperationNotAllowedErrors.readOnlyTable.message)
    }
  }

  /**
    * write table in a specific partition path
    *
    * @param df             {DataFrame}
    * @param table          {DataTable}
    * @param partitionKey   {String}
    * @param partitionValue {String}
    */
  protected def writePartition(df: DataFrame, table: DataTable, partitionKey: String, partitionValue: Any): Unit = {
    if (!table.isReadOnly) {
      df.createPartition(table.outputPath + table.name, Seq((partitionKey, partitionValue)), table.format, table.writeMode,table.properties)
    } else {
      throw new TableOperationNotAllowedException(TableOperationNotAllowedErrors.readOnlyTable.code,
        TableOperationNotAllowedErrors.readOnlyTable.message)
    }
  }

  /**
    * write table in a specific path with timestamp
    *
    * @param df             {DataFrame}
    * @param table          {DataTable}
    * @param partitionKey   {String}
    * @param partitionValue {String}
    */
  protected def writePartitionWithTS(df: DataFrame, table: DataTable, partitionKey: String, partitionValue: Any, timestampColumn: String): Unit = {
    if (!table.isReadOnly) {
      writePartition(df.withTS(timestampColumn), table, partitionKey, partitionValue)
    } else {
      throw new TableOperationNotAllowedException(TableOperationNotAllowedErrors.readOnlyTable.code,
        TableOperationNotAllowedErrors.readOnlyTable.message)
    }
  }

  /**
    * write table in a specific partition path
    *
    * @param df         {DataFrame}
    * @param table      {DataTable}
    * @param partitions {Seq[(String, Any)]}
    */
  protected def writeDeepPartition(df: DataFrame, table: DataTable, partitions: Seq[(String, Any)]): Unit = {
    if (!table.isReadOnly) {
      table.partitionColumns.zip(partitions).foreach(p => if (!p._1.equals(p._2._1)) {
        throw new PartitionNotFoundException(PartitionNotFoundErrors.partitionNotFoundError.code,
          PartitionNotFoundErrors.partitionNotFoundError.message.format(table.name, p._2._1))
      })
      df.createPartition(table.outputPath + table.name, partitions, table.format, table.writeMode, table.properties)
    } else {
      throw new TableOperationNotAllowedException(TableOperationNotAllowedErrors.readOnlyTable.code,
        TableOperationNotAllowedErrors.readOnlyTable.message)
    }
  }


  /**
    * write table in a specific partition path with timestamp
    *
    * @param df         {DataFrame}
    * @param table      {DataTable}
    * @param partitions {Seq[(String, Any)]}
    */
  protected def writeDeepPartitionWithTS(df: DataFrame, table: DataTable, partitions: Seq[(String, Any)], timestampColumn: String): Unit = {
    if (!table.isReadOnly) {
      writeDeepPartition(df.withTS(timestampColumn), table, partitions)
    } else {
      throw new TableOperationNotAllowedException(TableOperationNotAllowedErrors.readOnlyTable.code,
        TableOperationNotAllowedErrors.readOnlyTable.message)
    }
  }


}
