package com.jrgv89.sparkApp

trait Constants {
  val NAMENODE: String = "namenode"
  val PKS = "pks"
  val PATH = "path"
  val SORT_PKS = "sortPks"
  val NAME = "name"
  val SCHEMA = "schema"
  val WRITE_MODE = "writeMode"
  val FORMAT = "fileType"
  val OPTIONS = "options"
  val DEFAULT_FORMAT = "parquet"
  val DEFAULT_WRITEMODE = "overwrite"
  val PARTITION_COLUMNS = "partitionColumns"
  val CONFIG_TABLES = "tables"
  val CONFIG_APP: String = "app"
  val CONFIG_SPARK = "spark"
  val CONFIG_HDFS = "hdfs"
  val CONFIG_CONFIG = "config"
  val PROPERTIES = "properties"
  val ENABLE = "enable"
  val DEBUG = "debug"
  val DEFAULT_TABLE = "defaultTable"
  val TABLES = "tables"
  val INCLUDE_PARTITIONS_AS_PK = "includePartitionsAsPk"

  val SPARK_APP = "sparkApp"

  //paths

  val DEFAULT_TABLE_PATH = "config.defaultTable"
  val SPARK_JOB_NAME_PATH = "config.name"
  val SPARK_JOB_CONFIG_PATH = "config.spark"
  val OPTIONS_PATH = "config.options"
  val TABLES_PATH = "config.tables"
  val HDFS_PATH = "config.hdfs"
  val HDFS_ENABLE_PATH = "config.hdfs.enable"
  val HDFS_PROPERTIES_PATH = "config.hdfs.properties"


}
