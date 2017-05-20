package com.datiobd.spider.commons.crudOps.tableOps

import com.datiobd.spider.commons.crudOps.dataframeOps.DataframeUpdater
import com.datiobd.spider.commons.table.Table
import com.datiobd.spider.commons.utils.Utils
import org.apache.spark.sql.DataFrame

/**
  * Created by JRGv89 on 19/05/2017.
  */
trait TableUpdater extends DataframeUpdater with TableReader {

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
      readTable(df.sqlContext, table).schema
    } else {
      table.schema.get
    }
    Utils.areEqual(schema, df.schema)
    writeDF(df, table.path + TEMP_DIR_PATH + table.name, table.format, table.writeMode, table.properties, table.partitionColumns)
    moveDirectory(table.path + TEMP_DIR_PATH + table.name, table.path + table.name)
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
      readTable(df.sqlContext, table).schema
    } else {
      table.schema.get
    }
    Utils.areEqual(schema, df.schema)
    table.partitionColumns.zip(partitions).foreach(p => if (!p._1.equals(p._2._1)) {
      throw new Exception(s"table ${table.name} has not partition column ${p._2._1}")
    })
    val partitionPath = createDeepPartition(partitions)
    writeDF(df, table.path + TEMP_DIR_PATH + table.name, table.format, table.writeMode, table.properties, Seq())
    moveDirectory(table.path + TEMP_DIR_PATH + table.name, table.path + table.name + partitionPath)
  }


}
