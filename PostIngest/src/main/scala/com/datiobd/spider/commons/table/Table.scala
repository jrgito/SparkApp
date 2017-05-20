package com.datiobd.spider.commons.table

import com.datiobd.spider.commons.crudOps.tableOps.TableOps
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.StructType

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
           )  {


  def read(sqlContext: SQLContext, df: DataFrame) = {

  }

  def write(sqlContext: SQLContext, df: DataFrame) = {

  }

  def update(sqlContext: SQLContext, df: DataFrame) = {

  }

  def delete(sqlContext: SQLContext, df: DataFrame) = {

  }

  def readPartition(sqlContext: SQLContext, df: DataFrame) = {

  }

  def writePartition(sqlContext: SQLContext, df: DataFrame) = {

  }

  def updatePartition(sqlContext: SQLContext, df: DataFrame) = {

  }

  def deletePartition(sqlContext: SQLContext, df: DataFrame) = {

  }
}


