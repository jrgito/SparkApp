package com.datiobd.spider.commons

import com.typesafe.config.Config
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._


case class Table(name: String,
                 path: String,
                 format: String = "parquet",
                 writeMode: String = "overwrite",
                 pks: Seq[String],
                 partitions: Boolean,
                 partitionColumns: Seq[String],
                 properties: Option[Map[String, String]] = None,
                 var schema: Option[StructType]=None)

object TableBuilder extends Constants {

  def parseDefaultTable(config: Config): Map[String, Any] = {

    if (!config.hasPath("defaultTable")) {
      System.err.println("Warning!!:: default table not set.")
      System.err.println(
        """Something like this should be set in app.config
          |    defaultTable: {
          |      schema: "out"
          |      namenode: "/HDFS/bbva_spain/data/sfma/"
          |      path: "process/process-name"
          |      fileType: "parquet"
          |      writeMode: "overwrite"
          |      pks: []
          |      sortPks: false
          |      partitionColumns: [fec_cierre]
          |      properties: {
          |        readerOptions: {}
          |        writerOptions: {}
          |      }
          |    }
          |If not set, default table will be empty and may will give problems in the process.
        """.stripMargin)
      Map[String, String]()
    } else {
      config.getObject("defaultTable").unwrapped.asScala.toMap
    }
  }

  def parseConfigTables(config: Config, dt: Map[String, Any]): Map[String, Table] = {
    config.getConfigList(TABLES).asScala.map(x => {
      (x.getString(NAME), createTableWithDefaults(x.root.unwrapped.asScala.toMap, dt))
    }).toMap
  }

  def createTable(merged: Map[String, Any]): Table = {
    val partitionsColumns = Utils.toSeq[String](merged(PARTITION_COLUMNS))
    val includePartitions = merged.getOrElse(INCLUDE_PARTITIONS_AS_PK, false)
    val sortPks = merged.getOrElse(SORT_PKS, false)

    val pks: Seq[String] = (includePartitions, sortPks) match {
      case (false, false) => Utils.toSeq[String](merged(PKS))
      case (false, true) => Utils.toSeq[String](merged(PKS)).sortBy(pk => pk)
      case (true, false) => Utils.toSeq[String](merged(PKS)) ++ partitionsColumns
      case (true, true) => (Utils.toSeq[String](merged(PKS)) ++ partitionsColumns).sortBy(pk => pk)
    }

    Table(merged(NAME).toString,
      merged(PATH).toString,
      merged(FORMAT).toString,
      merged(WRITE_MODE).toString,
      pks,
      partitionsColumns.nonEmpty,
      partitionsColumns,
      Some(Utils.toMap[String](merged("properties"))))
  }

  def createTableWithDefaults(t: Map[String, Any], defaults: Map[String, Any]): Table = {
    createTable(defaults ++ t)
  }

}