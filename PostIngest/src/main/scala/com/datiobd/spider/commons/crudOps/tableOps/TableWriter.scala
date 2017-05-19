package com.datiobd.spider.commons.crudOps.tableOps

import com.datiobd.spider.commons.Utils
import com.datiobd.spider.commons.crudOps.Commons
import com.datiobd.spider.commons.crudOps.dataframeOps.{DataframeUpdater, DataframeWriter}
import com.datiobd.spider.commons.table.Table
import org.apache.spark.sql.{DataFrame, DataFrameWriter}

import scala.collection.Map

/**
  * Created by JRGv89 on 19/05/2017.
  */
trait TableWriter extends DataframeWriter {



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
    * write df with table properties
    *
    * @param df    {DataFrame}
    * @param table {Table}
    */
  def writeTable(df: DataFrame, table: Table): Unit = {
    writeDF(df, table.path + table.name, table.format, table.writeMode, table.properties, table.partitionColumns)
  }

}
