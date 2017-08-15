package com.jrgv89.sparkApp

import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * Created by JRGv89 on 20/05/2017.
  */
class Test(path: String) extends SparkApp(path) {
  override def execute(spark: SparkSession): Int = {


    import spark.implicits._

    val rows = Seq(("aa", "bb", 10), ("cc", "dd", 20))

    val df= rows.toDF("name", "surname", "age")

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
