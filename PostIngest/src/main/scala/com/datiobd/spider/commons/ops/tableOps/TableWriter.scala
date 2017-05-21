package com.datiobd.spider.commons.ops.tableOps

import com.datiobd.spider.commons.ops.dfOps.DFWriter
import com.datiobd.spider.commons.exceptions.{PartitionNotFoundErrors, PartitionNotFoundException}
import com.datiobd.spider.commons.table.Table
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
    writeDF(df, table.path + table.name, table.format, table.writeMode, table.properties, table.partitionColumns)
  }

  /**
    *
    * @param df             {DataFrame}
    * @param table          {Table}
    * @param partitionKey   {String}
    * @param partitionValue {String}
    */
  def writePartition(df: DataFrame, table: Table, partitionKey: String, partitionValue: Any): Unit = {
    writeDF(df, table.path + table.name + createPartition(partitionKey, partitionValue), table.format, table.writeMode, table.properties, Seq())
  }

  /**
    *
    * @param df         {DataFrame}
    * @param table      {Table}
    * @param partitions {Seq[(String, Any)]}
    */
  def writeDeepPartition(df: DataFrame, table: Table, partitions: Seq[(String, Any)]): Unit = {
    table.partitionColumns.zip(partitions).foreach(p => if (!p._1.equals(p._2._1)) {
      throw new PartitionNotFoundException(PartitionNotFoundErrors.partitionNotFoundError.code,
        PartitionNotFoundErrors.partitionNotFoundError.message.format(table.name, p._2._1))
    })
    writeDF(df, table.path + table.name + createDeepPartition(partitions), table.format, table.writeMode, table.properties, Seq())
  }


}
