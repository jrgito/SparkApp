package com.jrgv89.spark.utils.datatable.tableOps

import com.jrgv89.spark.utils.dataframe.Implicits._
import com.jrgv89.spark.utils.datatable.DataTable
import com.jrgv89.spark.utils.datatable.exceptions.{PartitionNotFoundErrors, PartitionNotFoundException}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.jrgv89.spark.utils.utils.Utils._
/**
  * Created by JRGv89 on 19/05/2017.
  */

protected trait TableReader {

  /**
    * read table with spark
    *
    * @param spark {SparkSession}
    * @param table {DataTable}
    * @return {DataFrame}
    */
  protected def readTable(spark: SparkSession, table: DataTable, changeSchema: Boolean = true): DataFrame = {
    val df = spark.get(Seq(table.inputPath + table.name), table.format, table.properties)
    if (changeSchema) table.schema = Some(df.schema)
    df
  }

  /**
    * read partition with spark
    *
    * @param spark          {SparkSession}
    * @param table          {DataTable}
    * @param partitionKey   {String}
    * @param partitionValue {String}
    * @return {DataFrame}
    */
  protected def readPartition(spark: SparkSession, table: DataTable, partitionKey: String, partitionValue: Any): DataFrame = {
    readDeepPartition(spark, table, Seq((partitionKey, partitionValue)))
  }

  /**
    * read deep partition with spark
    *
    * @param spark      {SparkSession}
    * @param table      {DataTable}
    * @param partitions {Seq[(String, Any)]}
    * @return
    */
  protected def readDeepPartition(spark: SparkSession, table: DataTable, partitions: Seq[(String, Any)]): DataFrame = {
    table.partitionColumns.zip(partitions).foreach(p => if (!p._1.equals(p._2._1)) {
      throw new PartitionNotFoundException(PartitionNotFoundErrors.partitionNotFoundError.code,
        PartitionNotFoundErrors.partitionNotFoundError.message.format(table.name, p._2._1))
    })
    val df = spark.getPartitions(Seq(table.inputPath + table.name), partitions, table.format, table.properties)
    table.schema = Some(df.schema)
    df
  }
}
