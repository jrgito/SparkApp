package com.datiobd.spider.commons.table

import com.datiobd.spider.commons.exceptions.{MandatoryKeyNotFound, MandatoryKeyNotFoundErrors}
import com.datiobd.spider.commons.utils.Utils
import com.typesafe.config.Config

import scala.collection.JavaConverters._

/**
  * Created by JRGv89 on 19/05/2017.
  */
class TableBuilder(defaults: Map[String, AnyRef]) {
  private val NAME = "name"
  private val PATH = "path"
  private val FORMAT = "format"
  private val MODE = "mode"
  private val PKS = "pks"
  private val PARTITION_COLUMNS = "partitionColumns"
  private val INCLUDE_PARTITIONS_AS_PK = "includePartitionsAsPk"
  private val SORT_PKS = "sortPks"
  private val PROPERTIES = "properties"
  //  {
  //    readerOptions: {}
  //    writerOptions: {}
  //  }
  private val keys = Seq(PATH, FORMAT, MODE, PKS, PARTITION_COLUMNS, INCLUDE_PARTITIONS_AS_PK, SORT_PKS, PROPERTIES)

  def this(config: Config) = {
    this(config.root.unwrapped.asScala.toMap)
    checkDefaults()
  }

  private def checkDefaults(): Unit = {
    keys.foreach(key => {
      if (!defaults.contains(key)) throw new MandatoryKeyNotFound(MandatoryKeyNotFoundErrors.mandatoryKeyNotFoundError.code,
        MandatoryKeyNotFoundErrors.mandatoryKeyNotFoundError.message.format(key))
    })
  }

  def tables(configs: Seq[Config]): Map[String, Table] = {
    configs.map(x => {
      (x.getString(NAME), table(x.root.unwrapped.asScala.toMap, defaults))
    }).toMap
  }

  def table(merged: Map[String, Any]): Table = {
    val partitionsColumns = Utils.toSeq[String](merged(PARTITION_COLUMNS))
    val includePartitions = merged.getOrElse(INCLUDE_PARTITIONS_AS_PK, false)
    val sortPks = merged.getOrElse(SORT_PKS, false)

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

  def table(t: Map[String, Any], defaults: Map[String, Any]): Table = {
    table(defaults ++ t)
  }

}

object TableBuilder {
  private var builder: Option[TableBuilder] = None

  def create: TableBuilder = builder.get

  private[commons] def instance(config: Config): TableBuilder = {
    if (builder.isEmpty) builder = Some(new TableBuilder(config))
    builder.get
  }

  private[commons] def instance(defaults: Map[String, AnyRef]): TableBuilder = {
    if (this.builder.isEmpty) this.builder = Some(new TableBuilder(defaults))
    this.builder.get
  }
}
