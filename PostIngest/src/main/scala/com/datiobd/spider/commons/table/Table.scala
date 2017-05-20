package com.datiobd.spider.commons.table

import com.datiobd.spider.commons.crudOps.tableOps.TableOps
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by JRGv89 on 19/05/2017.
  */
class Table(val name: String,
            val path: String,
            val format: String,
            val writeMode: String,
            val pks: Seq[String],
            val partitions: Boolean,
            val partitionColumns: Seq[String],
            val properties: Option[Map[String, String]] = None,
            var schema: Option[StructType] = None
           ) extends TableOps {


  def read(sqlContext: SQLContext): DataFrame = readTable(sqlContext, this)

  def write(df: DataFrame): Unit = writeTable(df, this)

  def update(df: DataFrame): Unit = updateTable(df, this)

  def delete(): Unit = {
    //TODO
  }

  def readPartition(sqlContext: SQLContext, partitionKey: String, partitionValue: Any): DataFrame =
    readPartition(sqlContext, this, partitionKey: String, partitionValue: Any)

  def readDeepPartition(sqlContext: SQLContext, partitions: Seq[(String, Any)]): DataFrame =
    readDeepPartition(sqlContext, this, partitions)

  def writePartition(df: DataFrame, partitionKey: String, partitionValue: Any): Unit = writePartition(df, this, partitionKey, partitionValue)

  def writeDeepPartition(df: DataFrame, partitions: Seq[(String, Any)]): Unit = writeDeepPartition(df, this, partitions)

  def updatePartition(df: DataFrame, partitionKey: String, partitionValue: Any): Unit = updatePartition(df, this, partitionKey, partitionValue)

  def updateDeepPartition(df: DataFrame, partitions: Seq[(String, Any)]): Unit = updateDeepPartition(df, this, partitions)

  def deletePartition(sqlContext: SQLContext, df: DataFrame): Unit = {
    //TODO
  }
  def debug(sqlContext: SQLContext): Unit = {
    if (isDebug()) {
      println("**************")
      println(s"* DEBUG: ${this.name}")
      println("**************")
      readTable(sqlContext, this, changeSchema = false).show
    }
  }
  def copy() : Table = this.clone().asInstanceOf[Table]

}


