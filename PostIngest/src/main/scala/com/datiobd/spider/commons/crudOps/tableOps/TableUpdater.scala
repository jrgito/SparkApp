package com.datiobd.spider.commons.crudOps.tableOps

import com.datiobd.spider.commons.crudOps.dfOps.DFUpdater
import com.datiobd.spider.commons.exceptions.{PartitionNotFoundErrors, PartitionNotFoundException}
import com.datiobd.spider.commons.table.Table
import com.datiobd.spider.commons.utils.{CheckDataFrame, Utils}
import org.apache.spark.sql.DataFrame

/**
  * Created by JRGv89 on 19/05/2017.
  */
trait TableUpdater extends DFUpdater with TableReader {

  /**
    * write df with table properties
    *
    * @param table {Table}
    * @param df    {DataFrame}
    */
  def updateTable(df: DataFrame, table: Table): Unit = {
    val schema = if (table.schema.isEmpty) {
      //TODO ADD slog
      println(s"Schema for table ${table.name} not set. Reading...")
      readTable(df.sqlContext, table, changeSchema = false).schema
    } else {
      table.schema.get
    }
    CheckDataFrame.areEqual(schema, df.schema)
    updateDF(df, table.path + table.name, table.format, table.properties, table.partitionColumns)
  }


  /**
    * update a partition
    *
    * @param df             {DataFrame}
    * @param table          {Table}
    * @param partitionKey   {String}
    * @param partitionValue {String}
    */
  def updatePartition(df: DataFrame, table: Table, partitionKey: String, partitionValue: Any): Unit = {
    updateDeepPartition(df, table, Seq((partitionKey, partitionValue)))
  }


  /**
    *
    * @param df         {DataFrame}
    * @param table      {Table}
    * @param partitions {Seq[(String, Any)]}
    */
  def updateDeepPartition(df: DataFrame, table: Table, partitions: Seq[(String, Any)]): Unit = {
    val schema = if (table.schema.isEmpty) {
      println(s"Schema for table ${table.name} not set. Reading...")
      readTable(df.sqlContext, table, changeSchema = false).schema
    } else {
      table.schema.get
    }
    CheckDataFrame.areEqual(schema, df.schema)
    table.partitionColumns.zip(partitions).foreach(p => if (!p._1.equals(p._2._1)) {
      throw new PartitionNotFoundException(PartitionNotFoundErrors.partitionNotFoundError.code,
        PartitionNotFoundErrors.partitionNotFoundError.message.format(table.name, p._2._1))
    })
    val partitionPath = createDeepPartition(partitions)
    updateDF(df, table.path + table.name + partitionPath, table.format, table.properties)
  }


}
