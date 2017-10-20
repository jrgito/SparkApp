package com.jrgv89.dataframe.dfOps

import com.jrgv89.sparkApp.ops.Commons
import com.jrgv89.sparkApp.utils.FileOps

/**
  * Created by JRGv89 on 19/05/2017.
  */
trait DFDeleter extends Commons with FileOps{
  /**
    * deletes a file according to the path
    *
    * @param path {String} path to delete
    */
  def deleteDF(path: String): Unit = deleteDirectory(path)
}
