package com.jrgv89.sparkApp.ops.tableOps

import com.jrgv89.sparkApp.ops.dfOps.DFDeleter
import com.jrgv89.sparkApp.table.Table

/**
  * Created by JRGv89 on 19/05/2017.
  */
protected trait TableDeleter extends DFDeleter {

  /**
    * delete a table directory
    *
    * @param table {Table} table to delete
    */
  protected  def deleteTable(table: Table): Unit = {
    deleteDF(table.inputPath + table.name)
  }

  /**
    *
    * delete a tables directory
    *
    * @param table          {Table} table to delete
    * @param partitionKey   {String} partition key
    * @param partitionValue {String} partition value
    */
  protected def deletePartition(table: Table, partitionKey: String, partitionValue: Any): Unit = {
    deleteDeepPartition(table, Seq((partitionKey, partitionValue)))
  }

  /**
    * delete a deep partition of a table given
    *
    * @param table      {Table} table
    * @param partitions {Seq[String, Any]} partitions of table to delete
    */
  protected def deleteDeepPartition(table: Table, partitions: Seq[(String, Any)]): Unit = {
    deleteDF(table.inputPath + table.name + createDeepPartition(partitions))
  }
}
