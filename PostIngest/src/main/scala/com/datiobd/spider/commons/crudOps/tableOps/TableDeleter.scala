package com.datiobd.spider.commons.crudOps.tableOps

import com.datiobd.spider.commons.crudOps.Commons
import com.datiobd.spider.commons.crudOps.dataframeOps.{DataframeDeleter, DataframeUpdater}
import com.datiobd.spider.commons.table.Table
import com.datiobd.spider.commons.utils.FileOps

/**
  * Created by JRGv89 on 19/05/2017.
  */
protected trait TableDeleter extends DataframeDeleter {

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

  /**
    * delete a deep partition of a table given
    *
    * @param table      {Table} table
    * @param partitions {Seq[String, Any]} partitions of table to delete
    */
  def deleteDeepPartition(table: Table, partitions: Seq[(String, Any)]): Unit = {
    deleteDirectory(table.path + table.name + createDeepPartition(partitions))
  }

}
