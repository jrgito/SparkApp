package com.datiobd.spider.commons.dataFrameOps

import com.datiobd.spider.commons.table

/**
  * Created by JRGv89 on 19/05/2017.
  */
protected  trait  DeleteOps {

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

}
