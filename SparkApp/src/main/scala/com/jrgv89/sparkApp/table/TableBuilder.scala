package com.jrgv89.sparkApp.table

import com.jrgv89.sparkApp.Loggeator
import com.jrgv89.sparkApp.exceptions.{MandatoryKeyNotFound, MandatoryKeyNotFoundErrors}
import com.typesafe.config.Config

import scala.collection.JavaConverters._

/**
  * Created by JRGv89 on 19/05/2017.
  */
class TableBuilder(defaults: Map[String, AnyRef]) extends Loggeator{

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
        MandatoryKeyNotFoundErrors.mandatoryKeyNotFoundError.message.format(key, "defaultTable"))
    })
  }

  def tables(configs: Seq[Config]): Map[String, Table] = {
    configs.map(x => {
      (x.getString(NAME), table(x.root.unwrapped.asScala.toMap, defaults))
    }).toMap
  }

  def table(t: Map[String, Any], defaults: Map[String, Any]): Table = {
    Table.apply(defaults ++ t)
  }

  override def clazz: Class[_] = this.getClass
}

object TableBuilder {
  private var builder: Option[TableBuilder] = None

  def instance: TableBuilder = builder.get

  private[sparkApp] def create(config: Config): TableBuilder = {
    if (builder.isEmpty) builder = Some(new TableBuilder(config))
    builder.get
  }

  private[sparkApp] def create(defaults: Map[String, AnyRef]): TableBuilder = {
    if (this.builder.isEmpty) this.builder = Some(new TableBuilder(defaults))
    this.builder.get
  }
}
