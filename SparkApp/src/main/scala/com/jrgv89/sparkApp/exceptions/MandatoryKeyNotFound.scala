package com.jrgv89.sparkApp.exceptions

import com.jrgv89.sparkApp.errors.Error

/**
  * Created by JRGv89 on 19/05/2017.
  */

class MandatoryKeyNotFound(code: Int, message: String) extends CodeException(code, message)

object MandatoryKeyNotFoundErrors {
  private val mandatoryKeyNotFoundCode = 600
  private val ioPathKeyNotFoundCode = 601
  val mandatoryKeyNotFoundError = Error(mandatoryKeyNotFoundCode, s"key %s not found in default table config")
  val ioPathKeyNotFoundError = Error(ioPathKeyNotFoundCode, s"input and output path both must exist")
}