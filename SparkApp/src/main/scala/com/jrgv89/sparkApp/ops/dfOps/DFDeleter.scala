package com.jrgv89.sparkApp.ops.dfOps

import com.jrgv89.sparkApp.ops.Commons
import com.jrgv89.sparkApp.utils.FileOps

/**
  * Created by JRGv89 on 19/05/2017.
  */
trait DFDeleter extends Commons {

  def deleteDF(path: String): Unit = FileOps.deleteDirectory(path)
}
