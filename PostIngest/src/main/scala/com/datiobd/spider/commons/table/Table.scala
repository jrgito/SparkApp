package com.datiobd.spider.commons.table

import org.apache.spark.sql.types.StructType

/**
  * Created by JRGv89 on 19/05/2017.
  */
case class Table(name: String,
                 path: String,
                 format: String,
                 writeMode: String,
                 pks: Seq[String],
                 partitions: Boolean,
                 partitionColumns: Seq[String],
                 properties: Option[Map[String, String]] = None,
                 var schema: Option[StructType] = None
                )


