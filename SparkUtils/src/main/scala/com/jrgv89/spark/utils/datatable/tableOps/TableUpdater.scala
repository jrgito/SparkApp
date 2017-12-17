package com.jrgv89.spark.utils.datatable.tableOps

import com.jrgv89.spark.utils.dataframe.Implicits._
import com.jrgv89.spark.utils.datatable.DataTable
import com.jrgv89.spark.utils.datatable.exceptions.{PartitionNotFoundErrors, PartitionNotFoundException, TableOperationNotAllowedErrors, TableOperationNotAllowedException}
import com.jrgv89.spark.utils.utils.CheckDataFrame
import com.jrgv89.spark.utils.utils.Utils._
import org.apache.spark.sql.DataFrame

/**
  * Created by JRGv89 on 19/05/2017.
  */
protected trait TableUpdater extends TableReader with TableWriter {

  /**
    * update table with df
    *
    * @param table {DataTable}
    * @param df    {DataFrame}
    */
  protected def updateTable(df: DataFrame, table: DataTable): Unit = {

    if (!table.isReadOnly) {
      val schema = if (table.schema.isEmpty) {
        //        loggeator.info(s"Schema for table ${table.name} not set. Reading...")
        readTable(df.sparkSession, table, changeSchema = false).schema
      } else {
        table.schema.get
      }
      CheckDataFrame.areEquals(schema, df.schema)
      df.update(table.inputPath + table.name, table.format, table.properties, table.partitionColumns)

    } else {
      throw new TableOperationNotAllowedException(TableOperationNotAllowedErrors.readOnlyTable.code,
        TableOperationNotAllowedErrors.readOnlyTable.message)
    }
  }

  /**
    * update table with df and timestamp
    *
    * @param table {DataTable}
    * @param df    {DataFrame}
    */
  protected def updateTableWithTS(df: DataFrame, table: DataTable, timestampColumn: String): Unit = {
    updateTable(df.withTS(timestampColumn), table)
  }


  /**
    * update a partition with df
    *
    * @param df             {DataFrame}
    * @param table          {DataTable}
    * @param partitionKey   {String}
    * @param partitionValue {String}
    */
  protected def updatePartition(df: DataFrame, table: DataTable, partitionKey: String, partitionValue: Any): Unit = {
    updateDeepPartition(df, table, Seq((partitionKey, partitionValue)))
  }

  /**
    * update a partition with df and timestamp
    *
    * @param df              {DataFrame}
    * @param table           {DataTable}
    * @param partitionKey    {String}
    * @param partitionValue  {String}
    * @param timestampColumn {String}
    */
  protected def updatePartitionWithTS(df: DataFrame, table: DataTable, partitionKey: String, partitionValue: Any, timestampColumn: String): Unit = {
    updateDeepPartition(df.withTS(timestampColumn), table, Seq((partitionKey, partitionValue)))
  }


  /**
    * update deep partition with df
    *
    * @param df         {DataFrame}
    * @param table      {DataTable}
    * @param partitions {Seq[(String, Any)]}
    */
  protected def updateDeepPartition(df: DataFrame, table: DataTable, partitions: Seq[(String, Any)]): Unit = {
    if (!table.isReadOnly) {
      val schema = if (table.schema.isEmpty) {
        //        loggeator.info(s"Schema for table ${table.name} not set. Reading...")
        readTable(df.sparkSession, table, changeSchema = false).schema
      } else {
        table.schema.get
      }
      CheckDataFrame.areEquals(schema, df.schema)
      table.partitionColumns.zip(partitions).foreach(p => if (!p._1.equals(p._2._1)) {
        throw new PartitionNotFoundException(PartitionNotFoundErrors.partitionNotFoundError.code,
          PartitionNotFoundErrors.partitionNotFoundError.message.format(table.name, p._2._1))
      })
      df.updatePartition(table.inputPath + table.name, partitions,table.format, table.properties)
    } else {
      throw new TableOperationNotAllowedException(TableOperationNotAllowedErrors.readOnlyTable.code,
        TableOperationNotAllowedErrors.readOnlyTable.message)
    }
  }

  /**
    * * update deep partition with df and timestamp
    *
    * @param df         {DataFrame}
    * @param table      {DataTable}
    * @param partitions {Seq[(String, Any)]}
    */
  protected def updateDeepPartitionWithTS(df: DataFrame, table: DataTable, partitions: Seq[(String, Any)], timestampColumn: String): Unit = {
    updateDeepPartition(df.withTS(timestampColumn), table, partitions)
  }


}
