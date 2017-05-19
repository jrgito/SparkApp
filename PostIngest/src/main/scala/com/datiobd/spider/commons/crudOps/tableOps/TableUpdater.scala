package com.datiobd.spider.commons.crudOps.tableOps

import com.datiobd.spider.commons.Utils
import com.datiobd.spider.commons.crudOps.Commons
import com.datiobd.spider.commons.crudOps.dataframeOps.DataframeUpdater
import com.datiobd.spider.commons.table.Table
import org.apache.spark.sql.DataFrame

/**
  * Created by JRGv89 on 19/05/2017.
  */
trait TableUpdater extends DataframeUpdater with TableReader{

  /**
    * write df with table properties
    *
    * @param table {Table}
    * @param df    {DataFrame}
    */
  def updateTable(df: DataFrame, table: Table): Unit = {
    val schema = if (table.schema.isEmpty) {
      //TODO ADD slog
      println(s"Schema for table ${table.name} not set. Reading...")
      readTable(df.sqlContext, table).schema
    } else {
      table.schema.get
    }
    Utils.areEqual(schema, df.schema)
    writeDF(df, table.path + TEMP_DIR_PATH + table.name, table.format, table.writeMode, table.properties, table.partitionColumns)
    moveDirectory(table.path + TEMP_DIR_PATH + table.name, table.path + table.name)
  }

}
