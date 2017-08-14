package com.jrgv89.sparkApp

import org.apache.spark.sql.SparkSession

/**
  * Created by JRGv89 on 20/05/2017.
  */
class Test(path: String) extends SparkApp(path) {
  override def execute(spark: SparkSession): Int = {
    println("test")
    tables("").read(spark)
    1
  }

  override def clazz: Class[_] = this.getClass

  loggeator.debug("s")
}

object Test extends App {
  new Test("SparkApp/src/main/resources/dummy.conf").start()

}
