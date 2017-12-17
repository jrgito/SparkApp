package com.jrgv89.spark.utils.datatable.tableOps

import com.jrgv89.spark.utils.dataframe.Implicits._
import com.jrgv89.spark.utils.datatable.DataTable
import org.apache.spark.sql.SparkSession

/**
  * Created by JRGv89 on 19/05/2017.
  */
protected trait TableDeleter {

  /**
    * delete a table directory
    *
    * @param table {DataTable} table to delete
    */
  protected def deleteTable(spark: SparkSession, table: DataTable): Unit = {
    spark.delete(Seq(table.inputPath + table.name))
  }

  /**
    *
    * delete a tables directory
    *
    * @param table          {DataTable} table to delete
    * @param partitionKey   {String} partition key
    * @param partitionValue {String} partition value
    */
  protected def deletePartition(spark: SparkSession, table: DataTable, partitionKey: String, partitionValue: Any): Unit = {
    deleteDeepPartition(spark, table, Seq((partitionKey, partitionValue)))
  }

  /**
    * delete a deep partition of a table given
    *
    * @param table      {DataTable} table
    * @param partitions {Seq[String, Any]} partitions of table to delete
    */
  protected def deleteDeepPartition(spark: SparkSession, table: DataTable, partitions: Seq[(String, Any)]): Unit = {
    spark.deletePartitions(Seq(table.inputPath + table.name), partitions)
  }
}
