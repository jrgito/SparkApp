package com.jrgv89.spark.utils.datatable.exceptions

import com.jrgv89.spark.utils.datatable.errors.Error


/**
  * Created by JRGv89 on 19/05/2017.
  */

class MandatoryKeyNotFound(code: Int, message: String) extends CodeException(code, message)

object MandatoryKeyNotFoundErrors {
  private val mandatoryKeyNotFoundCode = 601
  private val ioPathKeyNotFoundCode = 602
  val mandatoryKeyNotFoundError = Error(mandatoryKeyNotFoundCode, s"key %s not found in %s config")
  val ioPathKeyNotFoundError = Error(ioPathKeyNotFoundCode, s"input and output path both must exist")
}