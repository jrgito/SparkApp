package com.datiobd.spider.commons.ops.dfOps

import com.datiobd.spider.commons.ops.Commons
import com.datiobd.spider.commons.table.Table
import com.datiobd.spider.commons.utils.FileOps

/**
  * Created by JRGv89 on 19/05/2017.
  */
trait DFDeleter extends Commons {

  def deleteDF(path: String): Unit = FileOps.deleteDirectory(path)
}
