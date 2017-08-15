package com.jrgv89.sparkApp.utils

import java.io.File

import com.jrgv89.sparkApp.SparkAppConfig
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

trait FileOps {

  def getHDFSConfig: Configuration = SparkAppConfig.instance.hdfsConfiguration

  def isHDFSEnable: Boolean = SparkAppConfig.instance.isHdfsEnabled

  def deleteDirectory(path: String): Unit = {
    if (isHDFSEnable) {
      FileSystem.get(getHDFSConfig).delete(new Path(path), true)
    } else {
      FileUtils.deleteDirectory(new File(path))
    }
  }

  /**
    * rename source file in dest file. dest file will be removed before renaming
    *
    * @param source {String} source file path
    * @param dest   {String} dest file
    */

  def moveHDFSDir(source: String, dest: String): Unit = {
    val fs = FileSystem.get(getHDFSConfig)
    fs.delete(new Path(dest), true)
    fs.rename(new Path(source), new Path(dest))
  }

  /**
    * move local directory simulating spark update
    * @param source {String} source file path
    * @param dest   {String} dest file
    */
  def moveLocalDirectory(source: String, dest: String): Unit = {
    FileUtils.deleteDirectory(new File(dest))
    new File(source).renameTo(new File(dest))
  }

  def moveDirectory(source: String, dest: String): Unit = {
    if (isHDFSEnable) {
      moveHDFSDir(source, dest)
    } else {
      moveLocalDirectory(source, dest)
    }
  }

}
