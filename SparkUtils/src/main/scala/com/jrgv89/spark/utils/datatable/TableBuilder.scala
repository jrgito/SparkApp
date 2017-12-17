package com.jrgv89.spark.utils.datatable

import java.io.File

import com.jrgv89.spark.utils.datatable.exceptions.{MandatoryKeyNotFound, MandatoryKeyNotFoundErrors}
import com.typesafe.config.{Config, ConfigFactory}

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
        MandatoryKeyNotFoundErrors.mandatoryKeyNotFoundError.message.format(key, "defaultTable"))
    })
  }

  def tables(configs: Seq[Config]): Map[String, DataTable] = {
    configs.map(x => {
      (x.getString(NAME), table(x.root.unwrapped.asScala.toMap, defaults))
    }).toMap
  }

  def table(t: Map[String, Any], defaults: Map[String, Any]): DataTable = {
    DataTable.apply(defaults ++ t)
  }

  //  override def clazz: Class[_] = this.getClass
}

object TableBuilder {
  private var builder: Option[TableBuilder] = None

  def build(config: Config, path: String): Map[String, DataTable] = {
    val builder = create(config.getConfig(path + ".defaultTable"))
    builder.checkDefaults()
    builder.tables(config.getConfigList(path + ".tables").asScala)
  }

  def build(configFilePath: String, path: String): Map[String, DataTable] = {
    build(ConfigFactory.parseFile(new File(configFilePath)), path)
  }

  private def create(config: Config): TableBuilder = {
    if (builder.isEmpty) builder = Some(new TableBuilder(config))
    builder.get
  }

}
