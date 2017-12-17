package com.jrgv89.spark.utils.datatable.exceptions

/**
  * Created by JRGv89 on 20/05/2017.
  */
class CodeException(code: Int, message: String) extends Exception(message) {
  def getCode: Int = code
}
