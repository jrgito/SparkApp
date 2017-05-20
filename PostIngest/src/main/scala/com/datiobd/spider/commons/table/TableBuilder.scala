package com.datiobd.spider.commons.table

import com.datiobd.spider.commons.exceptions.MandatoryKeyNotFound
import com.datiobd.spider.commons.Utils
import com.typesafe.config.Config

import scala.collection.JavaConverters._

/**
  * Created by JRGv89 on 19/05/2017.
  */
private class TableBuilder(defaults: Map[String, AnyRef]) {
  val NAME = "name"
  val PATH = "path"
  val FORMAT = "format"
  val MODE = "MODE"
  val PKS = "pks"
  val PARTITION_COLUMNS = "partitionColumns"
  val INCLUDE_PARTITIONS_AS_PK = "includePartitionsAsPk"
  val SORT_PKS = "sortPks"
  val PROPERTIES = "properties"
  //  {
  //    readerOptions: {}
  //    writerOptions: {}
  //  }
  val keys = Seq(PATH, FORMAT, MODE, PKS, PARTITION_COLUMNS, INCLUDE_PARTITIONS_AS_PK, SORT_PKS, PROPERTIES)

  def this(config: Config) = {
    this(config.root.unwrapped.asScala.toMap)
    checkDefaults()
  }

  def checkDefaults(): Unit = {
    keys.foreach(key => {
      if (!defaults.contains(key)) throw new MandatoryKeyNotFound(s"key $key not found in default table config")
    })
  }

  def createTables(configs: Seq[Config]): Map[String, Table] = {
    configs.map(x => {
      (x.getString(NAME), createTable(x.root.unwrapped.asScala.toMap, defaults))
    }).toMap
  }

  def createTable(merged: Map[String, Any]): Table = {
    val partitionsColumns = Utils.toSeq[String](merged(PARTITION_COLUMNS))
    val includePartitions = merged.getOrElse(INCLUDE_PARTITIONS_AS_PK, false)
    val sortPks = merged.getOrElse(sortPks, false)

    val pks: Seq[String] = (includePartitions, sortPks) match {
      case (false, false) => Utils.toSeq[String](merged(PKS))
      case (false, true) => Utils.toSeq[String](merged(PKS)).sortBy(pk => pk)
      case (true, false) => Utils.toSeq[String](merged(PKS)) ++ partitionsColumns
      case (true, true) => (Utils.toSeq[String](merged(PKS)) ++ partitionsColumns).sortBy(pk => pk)
    }

    new Table(merged(NAME).toString,
      merged(PATH).toString,
      merged(FORMAT).toString,
      merged(MODE).toString,
      pks,
      partitionsColumns.nonEmpty,
      partitionsColumns,
      Some(Utils.toMap[String](merged(PROPERTIES))))
  }

  def createTable(t: Map[String, Any], defaults: Map[String, Any]): Table = {
    createTable(defaults ++ t)
  }

}

object TableBuilder {
  private var defaults: Option[TableBuilder] = None

  def instance(): TableBuilder = defaults.get

  private[SparkApp] def instance(config: Config): TableBuilder = {
    if (defaults.isEmpty) defaults = Some(new TableBuilder(config))
    defaults.get
  }

  private[SparkApp] def instance(defaults: Map[String, AnyRef]): TableBuilder = {
    if (this.defaults.isEmpty) this.defaults = Some(new TableBuilder(defaults))
    this.defaults.get
  }
}
