package com.jrgv89.sparkApp.ops.dfOps

import com.jrgv89.sparkApp.utils.FileOps
import org.apache.spark.sql.DataFrame

/**
  * Created by JRGv89 on 19/05/2017.
  */
trait DFUpdater extends DFWriter {

  private val saveMode = "overwrite"

  /**
    * updates file in path with the df in a specific format
    *
    * @param df     {DataFrame}
    * @param path   {String}
    * @param format {String}
    */
  def updateDF(df: DataFrame, path: String, format: String): Unit = {
    writeDF(df, path + TEMP_DIR_PATH, format, saveMode, None, Seq())
    FileOps.moveDirectory(path + TEMP_DIR_PATH, path)
  }

  /**
    * updates file in path with the df in a specific format and properties
    *
    * @param df         {DataFrame}
    * @param path       {String}
    * @param format     {String}
    * @param properties {Option[Map[String, String\]\]}
    */
  def updateDF(df: DataFrame, path: String, format: String, properties: Option[Map[String, String]]): Unit = {
    writeDF(df, path + TEMP_DIR_PATH, format, saveMode, properties, Seq())
    FileOps.moveDirectory(path + TEMP_DIR_PATH, path)
  }

  /**
    * updates file in path with the df in a specific format and properties
    *
    * @param df         {DataFrame}
    * @param path       {String}
    * @param format     {String}
    * @param properties {Option[Map[String, String\]\]}
    * @param partitions {Seq[String]}
    */
  def updateDF(df: DataFrame, path: String, format: String, properties: Option[Map[String, String]], partitions: Seq[String]): Unit = {
    writeDF(df, path + TEMP_DIR_PATH, format, saveMode, properties, partitions)
    FileOps.moveDirectory(path + TEMP_DIR_PATH, path)
  }
}
