package com.datiobd.spider.commons.utils

import java.io.File

import com.datiobd.spider.commons.SparkAppConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

trait FileOps {

  def getHDFSConfig: Configuration = SparkAppConfig.hdfsConfig

  def isHDFSEnable: Boolean = SparkAppConfig.isHdfsEnable

  /**
    * deletes hdfs dir
    *
    * @param path {String} file path
    */
  def deleteHDFSDir(path: String): Unit = {
    FileSystem.get(getHDFSConfig).delete(new Path(path), true)
  }

  /**
    *
    * @param file {File}
    */
  def deleteLocalDir(file: File): Unit = {
    if (file.isDirectory) {
      for (i <- file.listFiles())
        deleteLocalDir(i)
    }
    file.delete()
  }

  def deleteDirectory(dest: String): Unit = {
    if (isHDFSEnable) {
      deleteHDFSDir(dest)
    } else {
      deleteLocalDir(new File(dest))
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

  def moveLocalDir(source: String, dest: String): Unit = {
    deleteLocalDir(new File(dest))
    new File(source).renameTo(new File(dest))
  }

  def moveDirectory(source: String, dest: String): Unit = {
    if (isHDFSEnable) {
      moveHDFSDir(source, dest)
    } else {
      moveLocalDir(source, dest)
    }
  }

}
