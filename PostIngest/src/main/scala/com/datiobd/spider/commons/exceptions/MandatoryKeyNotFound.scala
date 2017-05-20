package com.datiobd.spider.commons.exceptions

import com.datiobd.spider.commons.errors.Error

/**
  * Created by JRGv89 on 19/05/2017.
  */

class MandatoryKeyNotFound(code: Int, message: String) extends CodeException(code, message)

object MandatoryKeyNotFoundErrors {
  private val mandatoryKeyNotFoundCode = 600
  val mandatoryKeyNotFoundError = Error(mandatoryKeyNotFoundCode, s"key %1 not found in default table config")
}