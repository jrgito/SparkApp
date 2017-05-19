package com.datiobd.spider.commons.table

import com.datiobd.spider.commons.TableBuilder._
import com.datiobd.spider.commons.{Table, Utils}
import com.typesafe.config.Config

import scala.collection.JavaConverters._

/**
  * Created by JRGv89 on 19/05/2017.
  */
object TableBuilder {



  val path = "path"
  val format = "format"
  val mode = "mode"
  val pks = "pks"
  val partitionColumns = "partitionColumns"
  val includePartitionsAsPk = "includePartitionsAsPk"
  val sortPks = "sortPks"
  val properties = "properties"
  //  {
  //    readerOptions: {}
  //    writerOptions: {}
  //  }
  val keys = Seq(path, format, mode, pks, partitionColumns,includePartitionsAsPk,sortPks,properties)

  def parseDefaultTable(config: Config): Map[String, Any] = {

    keys.foreach(key => {
      if(!config.hasPath(key)) throw new Exception
    })


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

}
