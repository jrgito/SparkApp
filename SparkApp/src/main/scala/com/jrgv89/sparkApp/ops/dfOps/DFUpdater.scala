package com.jrgv89.sparkApp.ops.dfOps

import com.jrgv89.sparkApp.utils.FileOps
import org.apache.spark.sql.DataFrame

/**
  * Created by JRGv89 on 19/05/2017.
  */
trait DFUpdater extends DFWriter {

  private val saveMode = "overwrite"

  def updateDF(df: DataFrame, path: String, format: String): Unit = {
    writeDF(df, path + TEMP_DIR_PATH, format, saveMode, None, Seq())
    FileOps.moveDirectory(path + TEMP_DIR_PATH, path)
  }

  def updateDF(df: DataFrame, path: String, format: String, properties: Option[Map[String, String]]): Unit = {
    writeDF(df, path + TEMP_DIR_PATH, format, saveMode, properties, Seq())
    FileOps.moveDirectory(path + TEMP_DIR_PATH, path)
  }

  def updateDF(df: DataFrame, path: String, format: String, properties: Option[Map[String, String]], partitions:Seq[String]): Unit = {
    writeDF(df, path + TEMP_DIR_PATH, format, saveMode, properties, partitions)
    FileOps.moveDirectory(path + TEMP_DIR_PATH, path)
  }
}
