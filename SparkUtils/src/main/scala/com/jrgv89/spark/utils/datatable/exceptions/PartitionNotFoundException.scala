package com.jrgv89.spark.utils.datatable.exceptions

import com.jrgv89.spark.utils.datatable.errors.Error

/**
  * Created by JRGv89 on 20/05/2017.
  */
class PartitionNotFoundException(code: Int, message: String) extends CodeException(code, message)

object PartitionNotFoundErrors {
  private val partitionNotFoundCode = 400
  val partitionNotFoundError = Error(partitionNotFoundCode, s"table %1 has not partition column %2")
}
