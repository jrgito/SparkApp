package com.jrgv89.sparkApp.exceptions

import com.jrgv89.sparkApp.errors.Error

/**
  * Created by JRGv89 on 20/05/2017.
  */
class TableOperationNotAllowedException(code: Int, message: String) extends CodeException(code, message)

object TableOperationNotAllowedErrors {
  private val operationNotAllowed = 700
  private val columnsNameCode = 701
  private val columnsTypesCode = 702
  val readOnlyTable = Error(operationNotAllowed, "this table is read only")
  val columnsNameError = Error(columnsNameCode, "field %s not found in right schema %s")
  val columnsTypesError = Error(columnsTypesCode, "field %s has different type left: %s != right: %s")
}