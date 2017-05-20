package com.datiobd.spider.commons.exceptions

import com.datiobd.spider.commons.errors.Error

/**
  * Created by JRGv89 on 20/05/2017.
  */
class SchemaException(code: Int, message: String) extends CodeException(code, message)

object SchemaErrors {
  private val columnsNumberCode = 300
  private val columnsNameCode = 301
  private val columnsTypesCode = 302
  val columnsNumberError = Error(columnsNumberCode, "Columns numbers are distinct: right (%d) != left (%d)")
  val columnsNameError = Error(columnsNameCode, "field %s not found in right schema %s")
  val columnsTypesError = Error(columnsTypesCode, "field %s has different type left: %s != right: %s")
}