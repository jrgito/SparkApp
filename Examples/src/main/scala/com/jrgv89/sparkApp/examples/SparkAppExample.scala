package com.jrgv89.sparkApp.examples

import com.jrgv89.sparkApp.SparkApp
import org.apache.spark.sql.SparkSession

object SparkAppExample extends App {
  new SparkAppExample("Examples/src/main/resources/dummy.conf").start()
}

class SparkAppExample(configPath: String) extends SparkApp(configPath) {

  override def execute(spark: SparkSession): Int = {

    import spark.implicits._

    val table1dataDF = Seq(("aa", "bb", 10), ("cc", "dd", 20)).toDF("name", "surname", "age")
    val table2dataDF = Seq(("aa", "bb", 10), ("cc", "dd", 20)).toDF("name", "surname", "age")

    val table1name = appConfig.getString("table1")
    tables(table1name).write(table1dataDF)

    val df1 = tables(table1name).read(spark)

    tables("table2").write(table2dataDF)

    val df2 = tables("table2").read(spark)

    df1.show()
    df2.show()


    val df3 = tables("table2").readPartition(spark, "age", 10)

    df3.show

    val df4 = Seq(("XX", "xx", 30)).toDF("name", "surname", "age")

    df4.show()

    tables("table2").updatePartition(df4, "age", 10)

    val df5 = tables("table2").readPartition(spark, "age", 10)

    df5.show()
    val df6 = tables("table2").read(spark)

    df6.show()

    //Note: name and surname from df4 updates table but not partition key

    0
  }

  override def clazz: Class[_] = this.getClass
}

